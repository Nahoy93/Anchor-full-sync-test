@echo off
rem ============================================================
rem  Anchor-server for Ship of Harkinian (egen hostad)
rem  Andra losenordet nedan innan du startar!
rem ============================================================
cd /d "%~dp0"

set ANCHOR_PASSWORD=byt-till-ditt-losenord
set PORT=43383

echo Startar Anchor-servern pa port %PORT% ...
echo Skriv "list" for att se rum och spelare, "stop" for att stanga.
echo.
anchor.exe
pause
