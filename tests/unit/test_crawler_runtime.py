import csv
import json
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock
from zoneinfo import ZoneInfo

import pytest
import requests

from src.ingestion import crawl_runtime as runtime
from src.orchestration import pipeline


def response(status, retry_after=None):
    r = requests.Response(); r.status_code=status; r.url='https://example.test/page'
    r._content=b'{}';r._content_consumed=True
    if retry_after is not None:r.headers['Retry-After']=str(retry_after)
    return r


@pytest.mark.parametrize('status',[401,403])
def test_blocked_site_stops_without_retry(status):
    session=Mock();session.get.return_value=response(status)
    with pytest.raises(runtime.CrawlBlocked):
        runtime.request_response(session,'https://example.test',timeout=10,retries=3)
    assert session.get.call_count==1


def test_rate_limit_retries_are_bounded_and_retry_after_is_capped(monkeypatch):
    session=Mock();session.get.return_value=response(429,999)
    waits=[];monkeypatch.setattr(runtime.time,'sleep',waits.append)
    with pytest.raises(runtime.CrawlBlocked,match='429'):
        runtime.request_response(session,'https://example.test',timeout=10,retries=3)
    assert session.get.call_count==3 and waits==[30,30]


def test_transient_server_error_can_recover(monkeypatch):
    session=Mock();session.get.side_effect=[response(503),response(200)]
    monkeypatch.setattr(runtime.time,'sleep',lambda delay:None)
    assert runtime.request_response(session,'https://example.test',timeout=10,retries=3).status_code==200
    assert session.get.call_count==2


@pytest.mark.parametrize('payload',[
    {'data':[],'total':100,'skip':0,'count':0},
    {'data':[{'_id':'1'}],'total':10,'skip':0,'count':2},
    {'data':[],'total':True,'skip':0,'count':0},
    {'data':[{'_id':'1'}],'total':10,'skip':0,'count':1,'limit':1},
])
def test_invalid_or_repeated_page_offset_cannot_be_an_end(payload):
    with pytest.raises(ValueError):runtime.validate_nhadatvui_page(payload,2)


def test_verified_empty_end_is_distinct_from_missing_data():
    assert runtime.validate_nhadatvui_page({'data':[],'total':18,'skip':18,'count':0,'limit':18},2)


def guland_fixture(tmp_path,monkeypatch,pages=10):
    from src.ingestion.guland import guland_raw_crawler as c
    folder=tmp_path/'crawl'
    for key,value in {'OUTPUT_DIR':folder,'PAGES_DIR':folder/'pages','CSV_PATH':folder/'guland_raw.csv',
        'JSONL_PATH':folder/'guland_raw.jsonl','SUMMARY_PATH':folder/'crawl_summary.json',
        'START_PAGE':1,'PAGES_TO_CRAWL':pages,'DELAY_SECONDS':0,'RUN_TIMEOUT_SECONDS':120,
        'MAX_CONSECUTIVE_FAILURES':2}.items():monkeypatch.setattr(c,key,value)
    monkeypatch.setattr(c,'bootstrap_session',lambda:None)
    monkeypatch.setattr(c,'parse_page_html',lambda html,page: [] if not html else [{'listing_id':html,'title':'Test','_scraped_at':'2026-11-21T00:00:00Z'}])
    return c


def test_guland_stops_after_consecutive_failed_pages(tmp_path,monkeypatch):
    c=guland_fixture(tmp_path,monkeypatch);fetch=Mock(side_effect=ValueError('invalid page'))
    monkeypatch.setattr(c,'request_page',fetch)
    assert c.main()==1 and fetch.call_count==2
    s=json.loads(c.SUMMARY_PATH.read_text());assert s['status']=='FAIL' and s['records_written']==0


def test_guland_does_not_turn_partial_data_into_a_success(tmp_path,monkeypatch):
    c=guland_fixture(tmp_path,monkeypatch)
    fetch=Mock(side_effect=[{'data':'one','has_more':True},runtime.CrawlBlocked('HTTP 403')])
    monkeypatch.setattr(c,'request_page',fetch)
    assert c.main()==1 and fetch.call_count==2
    s=json.loads(c.SUMMARY_PATH.read_text());assert s['records_written']==1 and s['status']=='FAIL'
    assert c.CSV_PATH.exists()


def test_guland_repeated_page_does_not_inflate_record_counts(tmp_path,monkeypatch):
    c=guland_fixture(tmp_path,monkeypatch)
    fetch=Mock(return_value={'data':'same','has_more':True});monkeypatch.setattr(c,'request_page',fetch)
    assert c.main()==1 and fetch.call_count==2
    s=json.loads(c.SUMMARY_PATH.read_text());assert s['records_written']==1 and s['stop_reason']=='repeated listing page'


def test_guland_accepts_a_server_confirmed_end(tmp_path,monkeypatch):
    c=guland_fixture(tmp_path,monkeypatch)
    monkeypatch.setattr(c,'request_page',Mock(return_value={'data':'one','has_more':False}))
    assert c.main()==0
    s=json.loads(c.SUMMARY_PATH.read_text());assert s['end_of_listing'] and s['completed_pages']==[1]


def test_guland_interruption_keeps_failed_summary(tmp_path,monkeypatch):
    c=guland_fixture(tmp_path,monkeypatch)
    monkeypatch.setattr(c,'request_page',Mock(side_effect=KeyboardInterrupt()))
    assert c.main()==1 and json.loads(c.SUMMARY_PATH.read_text())['status']=='FAIL'


def valid_export(folder,source,batch):
    folder.mkdir(parents=True,exist_ok=True)
    with (folder/f'{source}_raw.csv').open('w',encoding='utf-8',newline='') as handle:
        writer=csv.DictWriter(handle,fieldnames=['listing_id','title','_scraped_at']);writer.writeheader()
        writer.writerow({'listing_id':'1','title':'Test','_scraped_at':'2026-11-21T00:00:00Z'})
    summary={'source':source,'run_id':batch,'status':'PASS','records_written':1,'requested_pages':2,
        'start_page':1,'completed_pages':[1,2],'failed_pages':[],'end_of_listing':False}
    (folder/'crawl_summary.json').write_text(json.dumps(summary),encoding='utf-8')
    return summary


@pytest.mark.parametrize('fault',['rows','missing_page','wrong_batch','running','missing_column','truncated_row'])
def test_publication_rejects_inconsistent_exports(tmp_path,fault):
    summary=valid_export(tmp_path,'guland','20261121')
    if fault=='rows':summary['records_written']=99
    elif fault=='missing_page':summary['completed_pages']=[1]
    elif fault=='wrong_batch':summary['run_id']='20261120'
    elif fault=='running':summary['status']='RUNNING'
    elif fault=='missing_column':(tmp_path/'guland_raw.csv').write_text('title\nTest\n',encoding='utf-8')
    else:(tmp_path/'guland_raw.csv').write_text('listing_id,title,_scraped_at\n1\n',encoding='utf-8')
    (tmp_path/'crawl_summary.json').write_text(json.dumps(summary),encoding='utf-8')
    with pytest.raises(ValueError):pipeline.validate_crawl_output(tmp_path,'guland','20261121',2)


def test_successful_export_is_published_and_frozen(tmp_path,monkeypatch):
    monkeypatch.setattr(pipeline,'PROJECT_ROOT',tmp_path);monkeypatch.setenv('GULAND_CRAWL_PAGES','2')
    batch=datetime.now(ZoneInfo('Asia/Ho_Chi_Minh')).strftime('%Y%m%d')
    calls=[]
    def worker(command,*,env,**kwargs):
        calls.append(command);valid_export(Path(env['CRAWL_OUTPUT_DIR']),'guland',batch)
    monkeypatch.setattr(pipeline.subprocess,'run',worker)
    pipeline.prepare_web_source('guland',batch)
    final=pipeline.snapshot_path('guland',batch)
    assert final.exists() and not list(final.parent.parent.glob('.*.crawl-*'))
    pipeline.prepare_web_source('guland',batch);assert len(calls)==1
    final.write_text('listing_id,title,_scraped_at\n2,changed,now\n')
    with pytest.raises(RuntimeError,match='changed'):pipeline.prepare_web_source('guland',batch)


def test_process_timeout_cannot_publish_partial_csv(tmp_path,monkeypatch):
    import subprocess
    monkeypatch.setattr(pipeline,'PROJECT_ROOT',tmp_path)
    batch=datetime.now(ZoneInfo('Asia/Ho_Chi_Minh')).strftime('%Y%m%d')
    def worker(command,*,env,**kwargs):
        valid_export(Path(env['CRAWL_OUTPUT_DIR']),'guland',batch)
        raise subprocess.TimeoutExpired(command,kwargs['timeout'])
    monkeypatch.setattr(pipeline.subprocess,'run',worker)
    with pytest.raises(RuntimeError,match='Incomplete'):pipeline.prepare_web_source('guland',batch)
    assert not pipeline.snapshot_path('guland',batch).exists()
    stage=next(pipeline.snapshot_path('guland',batch).parent.parent.glob('.*.crawl-*'))
    assert json.loads((stage/'crawl_summary.json').read_text())['status']=='FAIL'


def test_publication_preserves_a_batch_that_appears_during_the_crawl(tmp_path,monkeypatch):
    monkeypatch.setattr(pipeline,'PROJECT_ROOT',tmp_path);monkeypatch.setenv('GULAND_CRAWL_PAGES','2')
    batch=datetime.now(ZoneInfo('Asia/Ho_Chi_Minh')).strftime('%Y%m%d')
    final=pipeline.snapshot_path('guland',batch)
    def worker(command,*,env,**kwargs):
        valid_export(Path(env['CRAWL_OUTPUT_DIR']),'guland',batch)
        final.parent.mkdir();final.write_bytes(b'preserve external export')
    monkeypatch.setattr(pipeline.subprocess,'run',worker)
    with pytest.raises(RuntimeError,match='appeared'):pipeline.prepare_web_source('guland',batch)
    assert final.read_bytes()==b'preserve external export'


@pytest.mark.parametrize('mode',['empty','repeat','blocked','budget'])
def test_nhadatvui_failure_stops_without_a_success_marker(tmp_path,monkeypatch,mode):
    from src.ingestion.nhadatvui import nhadatvui_raw_crawler as c
    for key,value in {'OUTPUT_DIR':tmp_path,'PAGES_DIR':tmp_path/'pages','CSV_PATH':tmp_path/'nhadatvui_raw.csv',
        'JSONL_PATH':tmp_path/'nhadatvui_raw.jsonl','SUMMARY_PATH':tmp_path/'crawl_summary.json',
        'START_PAGE':1,'PAGES_TO_CRAWL':20,'DELAY_SECONDS':0,'IS_RESUME':False,
        'MAX_CONSECUTIVE_FAILURES':2,'RUN_TIMEOUT_SECONDS':120}.items():monkeypatch.setattr(c,key,value)
    monkeypatch.setattr(c,'setup_logging',lambda:None);monkeypatch.setattr(c,'log',lambda m:None)
    if mode=='budget':monkeypatch.setattr(c,'RUN_TIMEOUT_SECONDS',0)
    fetch=Mock(return_value=('fixture','https://example.test',200))
    if mode=='blocked':fetch.side_effect=runtime.CrawlBlocked('HTTP 403')
    monkeypatch.setattr(c,'request_page',fetch)
    payload={'data':[] if mode=='empty' else [{'_id':'one'}],'skip':0,'count':0 if mode=='empty' else 1,'total':100}
    monkeypatch.setattr(c,'parse_nhadatvui_result',lambda text:payload)
    assert c.main()==1
    summary=json.loads(c.SUMMARY_PATH.read_text())
    assert summary['status']=='FAIL' and fetch.call_count==(0 if mode=='budget' else 1 if mode=='blocked' else 2)


@pytest.mark.parametrize('other_state',['success','failed','upstream_failed'])
def test_input_gate_allows_independent_collection_but_never_processing(other_state):
    names=[f'acquisition.{t}' for t in pipeline.ACQUISITION_TASKS]+['publish_evidence']
    names += [f'{group}.{name}' for group,jobs in pipeline.STAGES.items() for name,_ in jobs]
    tasks=[{'task_id':name,'state':('failed' if name=='acquisition.prepare_guland' else
           other_state if name=='acquisition.prepare_nhadatvui' else 'upstream_failed')} for name in names]
    assert pipeline.failure_gate_tasks_valid(tasks)
    next(t for t in tasks if t['task_id']=='bronze.ingest_snapshots')['state']='success'
    assert not pipeline.failure_gate_tasks_valid(tasks)
    assert not pipeline.failure_gate_tasks_valid(tasks[:-1])
