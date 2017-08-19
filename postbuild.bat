set ConfigurationName=%~1
set TargetDir=%~2
set TargetName=%~3
if "%ConfigurationName%"=="ReleaseWithPDB" goto rename 
if "%ConfigurationName%"=="Debug" goto rename 
if "%ConfigurationName%"=="Release" goto rename
goto end

:rename
del "%TargetDir%"%TargetName%_ora.exe
ren "%TargetDir%"%TargetName%.exe %TargetName%_ora.exe

:end
