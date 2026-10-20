@echo off
rem ============================================================
rem  Anchor-server for Ship of Harkinian (egen hostad)
rem  Andra losenordet nedan innan du startar!
rem ============================================================
cd /d "%~dp0"

set ANCHOR_PASSWORD=byt-till-ditt-losenord
set PORT=43383

rem Krypterade anslutningar (TLS) ar pa som standard. Certifikatet skapas i mappen tls
rem forsta gangen, behall den mappen. Ta bort "rem" pa en rad nedan for att andra:
rem set ANCHOR_REQUIRE_TLS=1
rem set ANCHOR_TLS=0
rem Rorelser over UDP ar pa som standard (oppna porten for bade TCP och UDP):
rem set ANCHOR_UDP=0

echo Startar Anchor-servern pa port %PORT% ...
echo Skriv "list" for att se rum, spelare, ping och trafik, "stop" for att stanga.
echo.
anchor.exe
pause
