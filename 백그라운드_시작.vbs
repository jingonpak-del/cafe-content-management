' 카페 게시글 감시 서버를 창 없이 백그라운드로 실행합니다.
' pythonw.exe 로 실행하므로 콘솔 창이 전혀 뜨지 않습니다.
' 로그는 data\server.log 에 기록됩니다.
Dim shell, fso, path, pyw
Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
path = fso.GetParentFolderName(WScript.ScriptFullName)
shell.CurrentDirectory = path

pyw = "C:\Users\USER\AppData\Local\Programs\Python\Python313\pythonw.exe"
If Not fso.FileExists(pyw) Then pyw = "pythonw"   ' 경로가 다르면 PATH 에서 탐색

shell.Run """" & pyw & """ -m src.server --port 8010 --logfile """ & path & "\data\server.log""", 0, False
