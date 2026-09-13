@echo off
echo Starting FlowAtlas...

:: Start the server in a new window
start "FlowAtlas Server" cmd /k "cd /d %~dp0 && node server.js"

:: Wait 3 seconds for server to boot
timeout /t 3 /nobreak >nul

:: Start ngrok in a new window
start "FlowAtlas Tunnel" cmd /k "ngrok http 8787"

echo Both started! Check the ngrok window for your public URL.
