param([string]$In, [string]$OutDocx, [string]$OutPdf)
$word = New-Object -ComObject Word.Application
$word.Visible = $false
$word.DisplayAlerts = 0
try {
  $doc = $word.Documents.Open($In, $false, $false)
  foreach ($toc in $doc.TablesOfContents) { $toc.Update() }
  $doc.Fields.Update() | Out-Null
  foreach ($toc in $doc.TablesOfContents) { $toc.Update() }
  $doc.SaveAs2($OutDocx, 16)
  $doc.ExportAsFixedFormat($OutPdf, 17)
  "pages=" + $doc.ComputeStatistics(2)
  "tocs=" + $doc.TablesOfContents.Count
  $doc.Close($false)
} finally { $word.Quit() }
