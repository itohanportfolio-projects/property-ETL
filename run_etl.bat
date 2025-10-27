@echo off
cd /d "%~dp0"
call .my_env/Scripts/activate
python main.py