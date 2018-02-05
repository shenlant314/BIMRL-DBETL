set source=%~1

rem rm -rf D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release\Plugins
rem md D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release\Plugins
md D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release\Plugins\BIMRL_ETL.XplorerPlugin
md D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release\Plugins\BIMRL_ETLConfig.XplorerPlugin
md D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release\Plugins\BIMRL_Main.XplorerPlugin

if %source%==ReleaseWithPDB goto oraRel
if %source%==Release goto oraRel
if %source%==Debug goto oraDbg
if %source%==RelWPDB_Postgres goto pgRel
if %source%==Rel_Postgres goto pgRel 
if %source%==Debug_Postgres goto pgDbg
goto end

:oraRel
cd BIMRL_ETL.XplorerPlugin\bin_ora\%source%\
cp -urf * D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release_OraPlugin\Plugins\BIMRL_ETL.XplorerPlugin_ora\

cd ..\..\..\BIMRL_ETLConfig.XplorerPlugin\bin_ora\%source%\
cp -urf * D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release_OraPlugin\Plugins\BIMRL_ETLConfig.XplorerPlugin_ora\

cd ..\..\..\BIMRL_Main.XplorerPlugin\bin_ora\%source%\
cp -urf * D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release_OraPlugin\Plugins\BIMRL_Main.XplorerPlugin_ora\

cd ..\..\..\
md D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release_OraPlugin\script_ora
cp -urf script_ora\*.sql D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release_OraPlugin\script_ora
cp -urf PluginConfig.xml D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release_OraPlugin\Plugins\BIMRL_ETL.XplorerPlugin_ora
cp -urf PluginConfig.xml D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release_OraPlugin\Plugins\BIMRL_ETLConfig.XplorerPlugin_ora
cp -urf PluginConfig.xml D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release_OraPlugin\Plugins\BIMRL_Main.XplorerPlugin_ora
goto end

:oraDbg
cd BIMRL_ETL.XplorerPlugin\bin_ora\%source%\
cp -urf * D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release_OraPlugin\Plugins\BIMRL_ETL.XplorerPlugin_ora\

cd ..\..\..\BIMRL_ETLConfig.XplorerPlugin\bin_ora\%source%\
cp -urf * D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release_OraPlugin\Plugins\BIMRL_ETLConfig.XplorerPlugin_ora\

cd ..\..\..\BIMRL_Main.XplorerPlugin\bin_ora\%source%\
cp -urf * D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release_OraPlugin\Plugins\BIMRL_Main.XplorerPlugin_ora\

cd ..\..\..\
md D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release_OraPlugin\script_ora\
cp -urf script_ora\*.sql D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release_OraPlugin\script_ora\
cp -urf PluginConfig.xml D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release_OraPlugin\Plugins\BIMRL_ETL.XplorerPlugin_ora
cp -urf PluginConfig.xml D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release_OraPlugin\Plugins\BIMRL_ETLConfig.XplorerPlugin_ora
cp -urf PluginConfig.xml D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release_OraPlugin\Plugins\BIMRL_Main.XplorerPlugin_ora
goto end

:pgRel
cd BIMRL_ETL.XplorerPlugin\bin\%source%\
cp -urf * D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release\Plugins\BIMRL_ETL.XplorerPlugin\

cd ..\..\..\BIMRL_ETLConfig.XplorerPlugin\bin\%source%\
cp -urf * D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release\Plugins\BIMRL_ETLConfig.XplorerPlugin\

cd ..\..\..\BIMRL_Main.XplorerPlugin\bin\%source%\
cp -urf * D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release\Plugins\BIMRL_Main.XplorerPlugin\

cd ..\..\..\
md D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release\script_pg
cp -urf script_pg\*.sql D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release\script_pg
cp -urf PluginConfig.xml D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release\Plugins\BIMRL_ETL.XplorerPlugin
cp -urf PluginConfig.xml D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release\Plugins\BIMRL_ETLConfig.XplorerPlugin
cp -urf PluginConfig.xml D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Release\Plugins\BIMRL_Main.XplorerPlugin
goto end

:pgDbg
cd BIMRL_ETL.XplorerPlugin\bin\%source%\
cp -urf * D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Debug\Plugins\BIMRL_ETL.XplorerPlugin\

cd ..\..\..\BIMRL_ETLConfig.XplorerPlugin\bin\%source%\
cp -urf * D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Debug\Plugins\BIMRL_ETLConfig.XplorerPlugin\

cd ..\..\..\BIMRL_Main.XplorerPlugin\bin\%source%\
cp -urf * D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Debug\Plugins\BIMRL_Main.XplorerPlugin\

cd ..\..\..\
md D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Debug\script_pg
cp -urf script_pg\*.sql D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Debug\script_pg
cp -urf PluginConfig.xml D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Debug\Plugins\BIMRL_ETL.XplorerPlugin
cp -urf PluginConfig.xml D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Debug\Plugins\BIMRL_ETLConfig.XplorerPlugin
cp -urf PluginConfig.xml D:\Git-Repositories\Invicara\Xbim-forked\XbimWindowsUI\Output\Debug\Plugins\BIMRL_Main.XplorerPlugin
goto end

:end
