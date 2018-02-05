REM connect as db owner (postgres)

CREATE ROLE bimrl NOINHERIT NOREPLICATION LOGIN PASSWORD 'bimrl' IN ROLE postgres;
CREATE SCHEMA bimrl AUTHORIZATION bimrl;

run BIMRL-std-once.sql
GRANT SELECT, UPDATE, USAGE
  ON public.bimrl_federatedmodel_federatedid_seq TO PUBLIC;

run colorDict_ins.sql
run ins_objhier_IFC2x3_pg.sql
REM run Revit2017BuiltinCategories.sql
REM run Revit2017BuiltinCategories-all.sql
