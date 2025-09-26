@echo off
REM Get Azure access token and save to file
REM Make sure you're logged in with 'az login' first

echo Getting Azure access token...

az account get-access-token --resource https://storage.azure.com/ --query accessToken --output tsv > /lakehouse/default/Files/token.txt

if %ERRORLEVEL% EQU 0 (
    echo Token successfully saved to token.txt
) else (
    echo Error: Failed to get access token
    echo Make sure you're logged in with 'az login'
)

pause