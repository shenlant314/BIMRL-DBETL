create sequence seq_bimrl_modelinfo_0000;
grant select on seq_bimrl_modelinfo_0000 to public;

create table bimrl_element_0000 (
		  elementid varchar(22) not null, 
		  lineno integer,
		  elementtype varchar(64) not null, 
		  modelid integer not null, 
		  typeid varchar(22), 
		  name varchar(256),
		  longname varchar(256),
		  ownerhistoryid integer,
		  description varchar(256),
		  objecttype varchar(256),
		  tag varchar(256),
		  container varchar(22),
		  geometrybody_geomtype geom3dtype,
		  geometrybody jsonb, 
		  geometrybody_bbox point3d[2], 
		  geometrybody_bbox_centroid point3d, 
		  geometryfootprint_geomtype geom3dtype,
		  geometryfootprint jsonb, 
		  geometryaxis_geomtype geom3dtype,
		  geometryaxis jsonb, 
		  transform_col double precision[4][4],
		  obb_ecs coordsystem,
		  obb jsonb,
		  total_surface_area double precision,
		  primary key (elementid)
);
grant select, update, insert, delete on bimrl_element_0000 to public;

create view bimrl_elemwogeom_0000 as select elementid,lineno,elementtype,modelid,typeid,name,longname,ownerhistoryid,description,objecttype,tag,container,geometrybody_bbox,geometrybody_bbox_centroid from bimrl_element_0000;

grant select, update, insert, delete on bimrl_elemwogeom_0000 to public;

create table bimrl_topo_face_0000 (
		  elementid varchar(22) not null,
		  id	varchar(22) not null,
		  type varchar(8) not null,
		  polygon jsonb not null,
		  normal point3d not null,
		  anglefromnorth	double precision,
		  centroid point3d not null,
		  orientation varchar(16),
		  attribute varchar(128),
		  toporbottom_z double precision
);

grant select, update, insert, delete on bimrl_topo_face_0000 to public;

create view bimrl_topofacev_0000 as select a.elementid,a.id,a.type,a.polygon,a.normal,a.anglefromnorth,a.centroid,a.orientation,a.attribute,b.elementtype from bimrl_topo_face_0000 a, bimrl_element_0000 b where a.elementid=b.elementid;

grant select, update, insert, delete on bimrl_topofacev_0000 to public;

create table bimrl_ownerhistory_0000 (
		  id	int,
		  modelid integer,
		  owningpersonname	varchar(256),
		  owningpersonroles	varchar(256),
		  owningpersonaddresses varchar(1024),
		  owningorganizationid	varchar(256),
		  owningorganizationname	varchar(256),
		  owningorganizationdescription varchar(256),
		  owningorganizationroles varchar(256),
		  owningorganizationaddresses varchar(1024),
		  applicationname varchar(256),
		  applicationversion varchar(256),
		  applicationdeveloper varchar(256),
		  applicationid varchar(256),
		  state varchar(16),
		  changeaction varchar(16),
		  lastmodifieddate date,
		  lastmodifyinguserid varchar(256),
		  lastmodifyingapplicationid varchar(256),
		  creationdate	date,
		  primary key (modelid, id)
);

grant select, update, insert, delete on bimrl_ownerhistory_0000 to public;

create table bimrl_spatialstructure_0000 (
		  spatialelementid varchar(22) not null, 
        spatialelementtype varchar(64) not null,
		  parentid varchar(22),
		  parenttype varchar(64),
		  levelremoved integer not null
);
grant select, update, insert, delete on bimrl_spatialstructure_0000 to public;

create table bimrl_modelinfo_0000 (
		  modelid integer not null, 
		  modelname varchar(256) not null, 
		  source varchar(256) not null, 
		  location point3d, 
		  transformation double precision[4][4], 
		  scale point3d, 
		  primary key (modelid)
);
grant select, update, insert, delete on bimrl_modelinfo_0000 to public;

create table bimrl_relconnection_0000 (
		  connectingelementid varchar(22) not null, 
		  connectingelementtype varchar(64) not null, 
		  connectingelementattrname varchar(128), 
		  connectingelementattrvalue varchar(256), 
		  connectedelementid varchar(22) not null, 
		  connectedelementtype varchar(64) not null, 
		  connectedelementattrname varchar(128), 
		  connectedelementattrvalue varchar(256), 
		  connectionattrname varchar(128), 
		  connectionattrvalue varchar(256), 
		  realizingelementid varchar(22),
		  realizingelementtype varchar(64),
		  relationshiptype varchar(64) not null 
);
grant select, update, insert, delete on bimrl_relconnection_0000 to public;

create table bimrl_elementproperties_0000 (
		  elementid varchar(22) not null, 
		  propertygroupname varchar(256) not null, 
		  propertyname varchar(256) not null, 
		  propertyvalue varchar(1024), 
		  propertydatatype varchar(128), 
		  propertyunit varchar(64)
);
grant select, update, insert, delete on bimrl_elementproperties_0000 to public;

create table bimrl_typematerial_0000 (
		  elementid varchar(22) not null, 
		  materialname varchar(256) not null, 
		  category varchar(256), 
		  setname varchar(256), 
		  materialsequence integer, 
		  materialthickness double precision, 
		  isventilated varchar(16),
		  forprofile varchar(256)
);
grant select, update, insert, delete on bimrl_typematerial_0000 to public;

create table bimrl_elementmaterial_0000 (
		  elementid varchar(22) not null, 
		  materialname varchar(256) not null, 
		  category varchar(256), 
		  setname varchar(256),
		  materialsequence integer, 
	     materialthickness double precision, 
		  isventilated varchar(16),
		  forprofile varchar(256)
);
grant select, update, insert, delete on bimrl_elementmaterial_0000 to public;

create table bimrl_elemclassification_0000 (
		  elementid varchar(22) not null,
		  classificationname varchar(256) not null,
		  classificationitemcode varchar(256) not null
);
grant select, update, insert, delete on bimrl_elemclassification_0000 to public;

create table bimrl_classification_0000 (
		  classificationname varchar(256) not null, 
		  classificationsource varchar(256), 
		  classificationedition varchar(256), 
		  classificationeditiondate date, 
		  classificationitemcode varchar(256) not null, 
		  classificationitemname varchar(256), 
		  classificationitemlocation varchar(256), 
		  primary key (classificationname, classificationitemcode)
);
grant select, update, insert, delete on bimrl_classification_0000 to public;

create table bimrl_type_0000 (
		  elementid varchar(22) not null,
		  ifctype varchar(64) not null,
		  name varchar(256) not null, 
		  description varchar(256),
 		  ownerhistoryid integer,
		  modelid integer,
		  applicableoccurrence	varchar(256),
		  tag varchar(256),
		  elementtype varchar(256),
		  predefinedtype varchar(256),	
		  assemblyplace varchar(256),	
		  operationtype varchar(256),	
		  constructiontype varchar(256),	
		  primary key (elementid)
);
grant select, update, insert, delete on bimrl_type_0000 to public;

create table bimrl_typclassification_0000 (
		  elementid varchar(22) not null,
		  classificationname varchar(256) not null,
		  classificationitemcode varchar(256) not null
);
grant select, update, insert, delete on bimrl_typclassification_0000 to public;

create view bimrl_classifassignment_0000 (
		  elementid, 
		  classificationname, 
		  classificationitemcode, 
		  classificationitemname, 
		  classificationitemlocation, 
		  classificationsource, 
		  classificationedition, 
		  fromtype) as 
(select a.elementid, a.classificationname, a.classificationitemcode, b.classificationitemname, 
		  b.classificationitemlocation, b.classificationsource, b.classificationedition, 'false'
		from bimrl_elemclassification_0000 a, bimrl_classification_0000 b 
			where b.classificationname=a.classificationname and b.classificationitemcode=a.classificationitemcode)
union
(select e.elementid, a.classificationname, a.classificationitemcode, b.classificationitemname, 
		  b.classificationitemlocation, b.classificationsource, b.classificationedition, 'true'
  		from bimrl_typclassification_0000 a, bimrl_classification_0000 b, bimrl_element_0000 e 
			where b.classificationname=a.classificationname and b.classificationitemcode=a.classificationitemcode
			and a.elementid=e.typeid)
;
grant select, update, insert, delete on bimrl_classifassignment_0000 to public;

create table bimrl_spatialindex_0000 (
		  elementid varchar(22) not null, 
		  cellid varchar(12) not null, 
		  xminbound	integer,
		  yminbound	integer,
		  zminbound integer,
		  xmaxbound integer,
		  ymaxbound integer,
		  zmaxbound integer,
		  depth integer,
		  primary key (elementid, cellid)
);
grant select, update, insert, delete on bimrl_spatialindex_0000 to public;

create table bimrl_typeproperties_0000 (
		  elementid varchar(22) not null, 
		  propertygroupname varchar(256) not null, 
		  propertyname varchar(256) not null, 
		  propertyvalue varchar(1024), 
		  propertydatatype varchar(128),
		  propertyunit varchar(64) 
);
grant select, update, insert, delete on bimrl_typeproperties_0000 to public;

create view bimrl_properties_0000 (elementid, propertygroupname, propertyname, propertyvalue, propertydatatype, propertyunit, fromtype) as
	(select elementid, propertygroupname, propertyname, propertyvalue, propertydatatype, propertyunit, 'false'
		  from bimrl_elementproperties_0000)
union
	(select a.elementid, b.propertygroupname, b.propertyname, b.propertyvalue, b.propertydatatype, b.propertyunit, 'true'
		  from bimrl_element_0000 a, bimrl_typeproperties_0000 b where b.elementid=a.typeid);
grant select, update, insert, delete on bimrl_properties_0000 to public;

create table bimrl_relaggregation_0000 (
		  masterelementid varchar(22) not null, 
		  masterelementtype varchar(64) not null, 
		  aggregateelementid varchar(22) not null, 
		  aggregateelementtype varchar(64) not null, 
		  primary key (masterelementid, aggregateelementid)
);
grant select, update, insert, delete on bimrl_relaggregation_0000 to public;

create table bimrl_relspaceboundary_0000 (
		  spaceelementid varchar(22) not null, 
		  boundaryelementid varchar(22) not null, 
		  boundaryelementtype varchar(64) not null, 
		  boundarytype varchar(32), 
		  internalorexternal varchar(32),
		  primary key (spaceelementid, boundaryelementid)
);
grant select, update, insert, delete on bimrl_relspaceboundary_0000 to public;

create table bimrl_relspaceb_detail_0000 (
		  spaceelementid varchar(22) not null,
		  sfaceboundid varchar(22) not null,
		  commonpointats point3d,
		  boundaryelementid varchar(22) not null,
		  bfaceboundid varchar(22) not null,
		  commonpointatb point3d,
		  sfacepolygon	jsonb not null,
		  sfacenormal point3d not null,
		  sfaceanglefromnorth double precision,
		  sfacecentroid point3d not null,
		  bfacepolygon jsonb not null,
		  bfacenormal point3d not null,
		  bfaceanglefromnorth double precision,
		  bfacecentroid point3d not null,
		  primary key (spaceelementid, sfaceboundid, boundaryelementid, bfaceboundid)
);
grant select, update, insert, delete on bimrl_relspaceb_detail_0000 to public;

create view bimrl_spaceboundaryv_0000 as select * from bimrl_relspaceboundary_0000 full join 
   (select a.*, b.elementtype boundaryelementtype from bimrl_relspaceb_detail_0000 a, bimrl_element_0000 b 
	where a.boundaryelementid=b.elementid) j using (spaceelementid, boundaryelementid, boundaryelementtype);
grant select, update, insert, delete on bimrl_spaceboundaryv_0000 to public;

create table bimrl_relgroup_0000 (
		  groupelementid varchar(22) not null, 
		  groupelementtype varchar(64) not null, 
		  memberelementid varchar(22) not null, 
		  memberelementtype varchar(64) not null, 
		  primary key (groupelementid, memberelementid)
);
grant select, update, insert, delete on bimrl_relgroup_0000 to public;

create table bimrl_elementdependency_0000 (
		  elementid varchar(22) not null, 
		  elementtype varchar(64) not null, 
		  dependentelementid varchar(22) not null,
		  dependentelementtype varchar(64) not null,
		  dependencytype varchar(32) not null,
		  primary key (elementid, dependentelementid)
);
grant select, update, insert, delete on bimrl_elementdependency_0000 to public;

create index idx_elementtype_0000 on bimrl_element_0000 (elementtype);

create index idx_topofeid_0000 on bimrl_topo_face_0000 (elementid);

create index bimrl_connectingelement_0000 on bimrl_relconnection_0000 (connectingelementid);

create index bimrl_connectedelement_0000 on bimrl_relconnection_0000 (connectedelementid);

create index idx_typmaterial_id_0000 on bimrl_typematerial_0000 (elementid);

create index idx_elemmaterial_id_0000 on bimrl_elementmaterial_0000 (elementid);

create index idx_spatial_cellid_0000 on bimrl_spatialindex_0000 (cellid);

create index ixminb_spatialindex_0000 on bimrl_spatialindex_0000 (xminbound);
create index iyminb_spatialindex_0000 on bimrl_spatialindex_0000 (yminbound);
create index izminb_spatialindex_0000 on bimrl_spatialindex_0000 (zminbound);
create index ixmaxb_spatialindex_0000 on bimrl_spatialindex_0000 (xmaxbound);
create index iymaxb_spatialindex_0000 on bimrl_spatialindex_0000 (ymaxbound);
create index izmaxb_spatialindex_0000 on bimrl_spatialindex_0000 (zmaxbound);

alter table bimrl_element_0000 add constraint fk_mmodelid_0000 foreign key (modelid) references bimrl_modelinfo_0000 (modelid);

alter table bimrl_element_0000 add constraint fk_typeid_0000 foreign key (typeid) references bimrl_type_0000 (elementid);
alter table bimrl_element_0000 add constraint fk_ownerhistid_0000 foreign key (modelid, ownerhistoryid) references bimrl_ownerhistory_0000 (modelid, id);
alter table bimrl_type_0000 add constraint fk_townerhistid_0000 foreign key (modelid, ownerhistoryid) references bimrl_ownerhistory_0000 (modelid, id);

alter table bimrl_typeproperties_0000 add constraint fk_typeprop_id_0000 foreign key (elementid) references bimrl_type_0000 (elementid);

alter table bimrl_elementproperties_0000 add constraint fk_elemprop_id_0000 foreign key (elementid) references bimrl_element_0000 (elementid);

alter table bimrl_relconnection_0000 add constraint fk_connecting_0000 foreign key (connectingelementid) references bimrl_element_0000 (elementid);
alter table bimrl_relconnection_0000 add constraint fk_connected_0000 foreign key (connectedelementid) references bimrl_element_0000 (elementid);
alter table bimrl_relconnection_0000 add constraint fk_realizing_0000 foreign key (realizingelementid) references bimrl_element_0000 (elementid);

alter table bimrl_elemclassification_0000 add constraint fk_classifelemid_0000 foreign key (elementid) references bimrl_element_0000 (elementid);
alter table bimrl_elemclassification_0000 add constraint fk_classifcode_0000 foreign key (classificationname, classificationitemcode) references bimrl_classification_0000 (classificationname, classificationitemcode);
alter table bimrl_typclassification_0000 add constraint fk_classiftelemid_0000 foreign key (elementid) references bimrl_type_0000 (elementid);
alter table bimrl_typclassification_0000 add constraint fk_classiftcode_0000 foreign key (classificationname, classificationitemcode) references bimrl_classification_0000 (classificationname, classificationitemcode);

alter table bimrl_elementmaterial_0000 add constraint fk_material_eid_0000 foreign key (elementid) references bimrl_element_0000 (elementid);

alter table bimrl_typematerial_0000 add constraint fk_typematerial_tid_0000 foreign key (elementid) references bimrl_type_0000 (elementid);

alter table bimrl_relaggregation_0000 add constraint fk_master_0000 foreign key (masterelementid) references bimrl_element_0000 (elementid);
alter table bimrl_relaggregation_0000 add constraint fk_aggregate_0000 foreign key (aggregateelementid) references bimrl_element_0000 (elementid);

alter table bimrl_relspaceboundary_0000 add constraint fk_space_0000 foreign key (spaceelementid) references bimrl_element_0000 (elementid);
alter table bimrl_relspaceboundary_0000 add constraint fk_boundaries_0000 foreign key (boundaryelementid) references bimrl_element_0000 (elementid);
alter table bimrl_relspaceb_detail_0000 add constraint fk_space_det_0000 foreign key (spaceelementid) references bimrl_element_0000 (elementid);
alter table bimrl_relspaceb_detail_0000 add constraint fk_boundaries_det_0000 foreign key (boundaryelementid) references bimrl_element_0000 (elementid);

alter table bimrl_spatialstructure_0000 add constraint fk_parent_0000 foreign key (parentid) references bimrl_element_0000 (elementid);
alter table bimrl_spatialstructure_0000 add constraint fk_spatialid_0000 foreign key (spatialelementid) references bimrl_element_0000 (elementid);

alter table bimrl_relgroup_0000 add constraint fk_group_0000 foreign key (groupelementid) references bimrl_element_0000 (elementid);
alter table bimrl_relgroup_0000 add constraint fk_member_0000 foreign key (memberelementid) references bimrl_element_0000 (elementid);

alter table bimrl_elementdependency_0000 add constraint fk_dependency_0000 foreign key (elementid) references bimrl_element_0000 (elementid);
alter table bimrl_elementdependency_0000 add constraint fk_dependency_eid_0000 foreign key (dependentelementid) references bimrl_element_0000 (elementid);
