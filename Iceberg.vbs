' ===== EDIT THIS =====
warehouse = "tpch/raw.Lakehouse"
' =====================

Set sh  = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

q = Chr(34)

' duckdb.exe lives next to the script (auto-downloaded on first run).
' Init file goes to C:\ProgramData so the path DuckDB prints on startup
' ("Loading resources from ...") does not leak the current username.
' The token file stays in the per-user %TEMP%: it holds a short-lived
' Entra access token and ProgramData is world-readable.
' start_quack.log goes next to the script.
scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)
duckdbExe = scriptDir & "\duckdb.exe"
initFile  = "C:\ProgramData\duckdb_init.sql"
logFile   = scriptDir & "\start_quack.log"

tmp     = sh.ExpandEnvironmentStrings("%TEMP%")
tokFile = tmp & "\az_tok.txt"
tok     = ""

' Download duckdb.exe if it isn't already next to the script
If Not fso.FileExists(duckdbExe) Then
    url     = "https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-windows-amd64.zip"
    zipPath = scriptDir & "\duckdb_cli.zip"
    ps = "powershell -NoProfile -ExecutionPolicy Bypass -Command " & q & _
         "$ProgressPreference='SilentlyContinue'; " & _
         "Invoke-WebRequest -Uri '" & url & "' -OutFile '" & zipPath & "'; " & _
         "Expand-Archive -LiteralPath '" & zipPath & "' -DestinationPath '" & scriptDir & "' -Force; " & _
         "Remove-Item -LiteralPath '" & zipPath & "'" & q
    sh.Run ps, 1, True
End If

Sub Log(msg)
    Dim lf
    Set lf = fso.OpenTextFile(logFile, 8, True)  ' 8 = ForAppending, create if missing
    lf.WriteLine Now & "  " & msg
    lf.Close
End Sub

Log "=== start ==="
Log "warehouse=" & warehouse

' Best-effort refresh of extensions from core_nightly. Each runs in its own
' duckdb process so a failure (e.g. extension not published there) can't
' affect the main session. Output goes to the log; errors are ignored.
Dim exts, ext, fiCmd
exts = Array("iceberg", "azure", "quack")
For Each ext In exts
    fiCmd = q & duckdbExe & q & " -c " & q & "FORCE INSTALL " & ext & " FROM core_nightly;" & q & _
            " >> " & q & logFile & q & " 2>&1"
    rc = sh.Run("cmd /c " & q & fiCmd & q, 0, True)
    Log "force install " & ext & " exit=" & rc
Next

' Check the Azure CLI is on PATH so we can give a clear message
' instead of a silent "empty token" failure.
rc = sh.Run("cmd /c where az >nul 2>&1", 0, True)
If rc <> 0 Then
    Log "ABORT: 'az' not found on PATH (install Azure CLI: https://aka.ms/installazurecliwindows)"
    MsgBox "'az' not found on PATH." & vbCrLf & _
           "Install Azure CLI: https://aka.ms/installazurecliwindows", _
           vbCritical, "Iceberg launcher"
    WScript.Quit 1
End If

' Fetch Entra token using current az login session (hidden, waits).
' stdout -> tokFile, stderr -> logFile
azCmd = "az account get-access-token --resource https://storage.azure.com/ --query accessToken -o tsv > " & q & tokFile & q & " 2>> " & q & logFile & q
rc = sh.Run("cmd /c " & q & azCmd & q, 0, True)
Log "az exit=" & rc

If fso.FileExists(tokFile) Then
    Set f = fso.OpenTextFile(tokFile, 1)
    If Not f.AtEndOfStream Then tok = Trim(f.ReadAll())
    f.Close
    fso.DeleteFile tokFile
End If
Log "token length=" & Len(tok)

If Len(tok) = 0 Then
    Log "ABORT: empty token (run 'az login' first)"
    MsgBox "Empty token. Run 'az login' first.", vbCritical, "Iceberg launcher"
    WScript.Quit 1
End If

' Write init file: set the variable, then the SQL to run
Set f = fso.CreateTextFile(initFile, True)
f.WriteLine ".print Serving warehouse: " & warehouse
f.WriteLine "SET VARIABLE onelake_tok = '" & tok & "';"
f.WriteLine "ATTACH OR REPLACE '" & warehouse & "' AS onelake ("
f.WriteLine "    TYPE ICEBERG,"
f.WriteLine "    ENDPOINT 'https://onelake.table.fabric.microsoft.com/iceberg',"
f.WriteLine "    TOKEN getvariable('onelake_tok'),"
f.WriteLine "    DEFAULT_SCHEMA dbo,"
f.WriteLine "    MAX_TABLE_STALENESS INTERVAL '5 minutes'"
f.WriteLine ");"
f.WriteLine "USE onelake;"
f.WriteLine "LOAD quack;"
f.WriteLine "CALL quack_serve('quack:127.0.0.1:9494', token => 'test');"
f.Close
Log "wrote " & initFile & "; launching duckdb"

' Launch duckdb in a visible console. No stderr redirect — errors must show inline.
sh.CurrentDirectory = scriptDir
sh.Run q & duckdbExe & q & " -init " & q & initFile & q, 1, False
