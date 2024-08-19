@echo off

set user=%1
set password=%2
set export_name=%3
set output_path=%4

tabcmd login -s https://prtableau.e-470.com -u %user% -p %password%

tabcmd get "%export_name%.png" --filename "%output_path%"

tabcmd logout