REM For Oracle version
set GITREPO=D:\Git_Repository

rm -rf %GITREPO%\Xbim-Invicara\XbimWindowsUI\Output\ReleaseWithPDB\Plugins
md %GITREPO%\Xbim-Invicara\XbimWindowsUI\Output\ReleaseWithPDB\Plugins
md %GITREPO%\Xbim-Invicara\XbimWindowsUI\Output\ReleaseWithPDB\Plugins\BIMRL_ETL.XplorerPlugin
md %GITREPO%\Xbim-Invicara\XbimWindowsUI\Output\ReleaseWithPDB\Plugins\BIMRL_ETLConfig.XplorerPlugin
md %GITREPO%\Xbim-Invicara\XbimWindowsUI\Output\ReleaseWithPDB\Plugins\BIMRL_Main.XplorerPlugin

cd BIMRL_ETL.XplorerPlugin\bin_ora\ReleaseWithPDB\
cp -rf * %GITREPO%\Xbim-Invicara\XbimWindowsUI\Output\ReleaseWithPDB\Plugins\BIMRL_ETL.XplorerPlugin\

cd ..\..\..\BIMRL_ETLConfig.XplorerPlugin\bin_ora\ReleaseWithPDB\
cp -rf * %GITREPO%\Xbim-Invicara\XbimWindowsUI\Output\ReleaseWithPDB\Plugins\BIMRL_ETLConfig.XplorerPlugin\

cd ..\..\..\BIMRL_Main.XplorerPlugin\bin_ora\ReleaseWithPDB\
cp -rf * %GITREPO%\Xbim-Invicara\XbimWindowsUI\Output\ReleaseWithPDB\Plugins\BIMRL_Main.XplorerPlugin\

cd ..\..\..\
cp -rf script_ora %GITREPO%\Xbim-Invicara\XbimWindowsUI\Output\ReleaseWithPDB\
cp -rf PluginConfig.xml %GITREPO%\Xbim-Invicara\XbimWindowsUI\Output\ReleaseWithPDB\Plugins\BIMRL_ETL.XplorerPlugin
cp -rf PluginConfig.xml %GITREPO%\Xbim-Invicara\XbimWindowsUI\Output\ReleaseWithPDB\Plugins\BIMRL_ETLConfig.XplorerPlugin
cp -rf PluginConfig.xml %GITREPO%\Xbim-Invicara\XbimWindowsUI\Output\ReleaseWithPDB\Plugins\BIMRL_Main.XplorerPlugin

REM For Postgres version
rm -rf %GITREPO%\Xbim-Invicara\XbimWindowsUI\Output\RelWPDB_Postgres\Plugins
md %GITREPO%\Xbim-Invicara\XbimWindowsUI\Output\RelWPDB_Postgres\Plugins
md %GITREPO%\Xbim-Invicara\XbimWindowsUI\Output\RelWPDB_Postgres\Plugins\BIMRL_ETL.XplorerPlugin
md %GITREPO%\Xbim-Invicara\XbimWindowsUI\Output\RelWPDB_Postgres\Plugins\BIMRL_ETLConfig.XplorerPlugin
md %GITREPO%\Xbim-Invicara\XbimWindowsUI\Output\RelWPDB_Postgres\Plugins\BIMRL_Main.XplorerPlugin

cd BIMRL_ETL.XplorerPlugin\bin\RelWPDB_Postgres\
cp -rf * %GITREPO%\Xbim-Invicara\XbimWindowsUI\Output\RelWPDB_Postgres\Plugins\BIMRL_ETL.XplorerPlugin\

cd ..\..\..\BIMRL_ETLConfig.XplorerPlugin\bin\RelWPDB_Postgres\
cp -rf * %GITREPO%\Xbim-Invicara\XbimWindowsUI\Output\RelWPDB_Postgres\Plugins\BIMRL_ETLConfig.XplorerPlugin\

cd ..\..\..\BIMRL_Main.XplorerPlugin\bin\RelWPDB_Postgres\
cp -rf * %GITREPO%\Xbim-Invicara\XbimWindowsUI\Output\RelWPDB_Postgres\Plugins\BIMRL_Main.XplorerPlugin\

cd ..\..\..\
cp -rf script_pg %GITREPO%\Xbim-Invicara\XbimWindowsUI\Output\RelWPDB_Postgres\
cp -rf PluginConfig.xml %GITREPO%\Xbim-Invicara\XbimWindowsUI\Output\RelWPDB_Postgres\Plugins\BIMRL_ETL.XplorerPlugin
cp -rf PluginConfig.xml %GITREPO%\Xbim-Invicara\XbimWindowsUI\Output\RelWPDB_Postgres\Plugins\BIMRL_ETLConfig.XplorerPlugin
cp -rf PluginConfig.xml %GITREPO%\Xbim-Invicara\XbimWindowsUI\Output\RelWPDB_Postgres\Plugins\BIMRL_Main.XplorerPlugin

