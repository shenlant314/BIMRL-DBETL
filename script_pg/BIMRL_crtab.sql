create sequence seq_bimrl_modelinfo_&1;
grant select on seq_bimrl_modelinfo_&1 to public;

create table bimrl_element_&1 (
		  elementid varchar(48) not null, 
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
		  container varchar(48),
		  geometrybody_geomtype geom3dtype,
		  geometrybody jsonb, 
		  geometrybody_bbox point3d[2], 
		  geometrybody_bbox_centroid point3d, 
		  geometryfootprint_geomtype geom3dtype,
		  geometryfootprint jsonb, 
		  geometryaxis_geomtype geom3dtype,
		  geometryaxis jsonb, 
		  transform_col double precision[4][4],
		  obb_ecs point3d[4],
		  obb jsonb,
		  total_surface_area double precision,
		  primary key (elementid)
);
grant select, update, insert, delete on bimrl_element_&1 to public;

create view bimrl_elemwogeom_&1 as select elementid,lineno,elementtype,modelid,typeid,name,longname,ownerhistoryid,description,objecttype,tag,container,geometrybody_bbox,geometrybody_bbox_centroid from bimrl_element_&1;

grant select, update, insert, delete on bimrl_elemwogeom_&1 to public;

create table bimrl_topo_face_&1 (
		  elementid varchar(48) not null,
		  id	varchar(48) not null,
		  type varchar(8) not null,
		  polygon jsonb not null,
		  normal point3d not null,
		  anglefromnorth	double precision,
		  centroid point3d not null,
		  orientation varchar(16),
		  attribute varchar(128),
		  toporbottom_z double precision
);

grant select, update, insert, delete on bimrl_topo_face_&1 to public;

create view bimrl_topofacev_&1 as select a.elementid,a.id,a.type,a.polygon,a.normal,a.anglefromnorth,a.centroid,a.orientation,a.attribute,b.elementtype from bimrl_topo_face_&1 a, bimrl_element_&1 b where a.elementid=b.elementid;

grant select, update, insert, delete on bimrl_topofacev_&1 to public;

create table bimrl_ownerhistory_&1 (
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

grant select, update, insert, delete on bimrl_ownerhistory_&1 to public;

create table bimrl_spatialstructure_&1 (
		  spatialelementid varchar(48) not null, 
        spatialelementtype varchar(64) not null,
		  parentid varchar(48),
		  parenttype varchar(64),
		  levelremoved integer not null
);
grant select, update, insert, delete on bimrl_spatialstructure_&1 to public;

create table bimrl_modelinfo_&1 (
		  modelid integer not null, 
		  modelname varchar(256) not null, 
		  source varchar(256) not null, 
		  location point3d, 
		  transformation double precision[4][4], 
		  scale point3d, 
		  primary key (modelid)
);
grant select, update, insert, delete on bimrl_modelinfo_&1 to public;

create table bimrl_relconnection_&1 (
		  connectingelementid varchar(48) not null, 
		  connectingelementtype varchar(64) not null, 
		  connectingelementattrname varchar(128), 
		  connectingelementattrvalue varchar(256), 
		  connectedelementid varchar(48) not null, 
		  connectedelementtype varchar(64) not null, 
		  connectedelementattrname varchar(128), 
		  connectedelementattrvalue varchar(256), 
		  connectionattrname varchar(128), 
		  connectionattrvalue varchar(256), 
		  realizingelementid varchar(48),
		  realizingelementtype varchar(64),
		  relationshiptype varchar(64) not null 
);
grant select, update, insert, delete on bimrl_relconnection_&1 to public;

create table bimrl_elementproperties_&1 (
		  elementid varchar(48) not null, 
		  propertygroupname varchar(256) not null, 
		  propertyname varchar(256) not null, 
		  propertyvalue varchar(1024), 
		  propertydatatype varchar(128), 
		  propertyunit varchar(64)
/*,
		  primary key (elementid, propertygroupname, propertyname)
*/
);
grant select, update, insert, delete on bimrl_elementproperties_&1 to public;

create table bimrl_typematerial_&1 (
		  elementid varchar(48) not null, 
		  materialname varchar(256) not null, 
		  category varchar(256), 
		  setname varchar(256), 
		  materialsequence integer, 
		  materialthickness double precision, 
		  isventilated varchar(16),
		  forprofile varchar(256)
);
grant select, update, insert, delete on bimrl_typematerial_&1 to public;

create table bimrl_elementmaterial_&1 (
		  elementid varchar(48) not null, 
		  materialname varchar(256) not null, 
		  category varchar(256), 
		  setname varchar(256),
		  materialsequence integer, 
	     materialthickness double precision, 
		  isventilated varchar(16),
		  forprofile varchar(256)
);
grant select, update, insert, delete on bimrl_elementmaterial_&1 to public;

create table bimrl_elemclassification_&1 (
		  elementid varchar(48) not null,
		  classificationname varchar(256) not null,
		  classificationitemcode varchar(256) not null
);
grant select, update, insert, delete on bimrl_elemclassification_&1 to public;

create table bimrl_classification_&1 (
		  classificationname varchar(256) not null, 
		  classificationsource varchar(256), 
		  classificationedition varchar(256), 
		  classificationeditiondate date, 
		  classificationitemcode varchar(256) not null, 
		  classificationitemname varchar(256), 
		  classificationitemlocation varchar(256), 
		  primary key (classificationname, classificationitemcode)
);
grant select, update, insert, delete on bimrl_classification_&1 to public;

create table bimrl_type_&1 (
		  elementid varchar(48) not null,
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
grant select, update, insert, delete on bimrl_type_&1 to public;

create table bimrl_typclassification_&1 (
		  elementid varchar(48) not null,
		  classificationname varchar(256) not null,
		  classificationitemcode varchar(256) not null
);
grant select, update, insert, delete on bimrl_typclassification_&1 to public;

create view bimrl_classifassignment_&1 (
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
		from bimrl_elemclassification_&1 a, bimrl_classification_&1 b 
			where b.classificationname=a.classificationname and b.classificationitemcode=a.classificationitemcode)
union
(select e.elementid, a.classificationname, a.classificationitemcode, b.classificationitemname, 
		  b.classificationitemlocation, b.classificationsource, b.classificationedition, 'true'
  		from bimrl_typclassification_&1 a, bimrl_classification_&1 b, bimrl_element_&1 e 
			where b.classificationname=a.classificationname and b.classificationitemcode=a.classificationitemcode
			and a.elementid=e.typeid)
;
grant select, update, insert, delete on bimrl_classifassignment_&1 to public;

create table bimrl_spatialindex_&1 (
		  elementid varchar(48) not null, 
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
grant select, update, insert, delete on bimrl_spatialindex_&1 to public;

create table bimrl_typeproperties_&1 (
		  elementid varchar(48) not null, 
		  propertygroupname varchar(256) not null, 
		  propertyname varchar(256) not null, 
		  propertyvalue varchar(1024), 
		  propertydatatype varchar(128),
		  propertyunit varchar(64) 
);
grant select, update, insert, delete on bimrl_typeproperties_&1 to public;

create view bimrl_properties_&1 (elementid, propertygroupname, propertyname, propertyvalue, propertydatatype, propertyunit, fromtype) as
	(select elementid, propertygroupname, propertyname, propertyvalue, propertydatatype, propertyunit, 'false'
		  from bimrl_elementproperties_&1)
union
	(select a.elementid, b.propertygroupname, b.propertyname, b.propertyvalue, b.propertydatatype, b.propertyunit, 'true'
		  from bimrl_element_&1 a, bimrl_typeproperties_&1 b where b.elementid=a.typeid);
grant select, update, insert, delete on bimrl_properties_&1 to public;

create table bimrl_relaggregation_&1 (
		  masterelementid varchar(48) not null, 
		  masterelementtype varchar(64) not null, 
		  aggregateelementid varchar(48) not null, 
		  aggregateelementtype varchar(64) not null, 
		  primary key (masterelementid, aggregateelementid)
);
grant select, update, insert, delete on bimrl_relaggregation_&1 to public;

create table bimrl_relspaceboundary_&1 (
		  spaceelementid varchar(48) not null, 
		  boundaryelementid varchar(48) not null, 
		  boundaryelementtype varchar(64) not null, 
		  boundarytype varchar(32), 
		  internalorexternal varchar(32),
		  primary key (spaceelementid, boundaryelementid)
);
grant select, update, insert, delete on bimrl_relspaceboundary_&1 to public;

create table bimrl_relspaceb_detail_&1 (
		  spaceelementid varchar(48) not null,
		  sfaceboundid varchar(48) not null,
		  commonpointats point3d,
		  boundaryelementid varchar(48) not null,
		  bfaceboundid varchar(48) not null,
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
grant select, update, insert, delete on bimrl_relspaceb_detail_&1 to public;

create view bimrl_spaceboundaryv_&1 as select * from bimrl_relspaceboundary_&1 full join 
   (select a.*, b.elementtype boundaryelementtype from bimrl_relspaceb_detail_&1 a, bimrl_element_&1 b 
	where a.boundaryelementid=b.elementid) j using (spaceelementid, boundaryelementid, boundaryelementtype);
grant select, update, insert, delete on bimrl_spaceboundaryv_&1 to public;

create table bimrl_relgroup_&1 (
		  groupelementid varchar(48) not null, 
		  groupelementtype varchar(64) not null, 
		  memberelementid varchar(48) not null, 
		  memberelementtype varchar(64) not null, 
		  primary key (groupelementid, memberelementid)
);
grant select, update, insert, delete on bimrl_relgroup_&1 to public;

create table bimrl_elementdependency_&1 (
		  elementid varchar(48) not null, 
		  elementtype varchar(64) not null, 
		  dependentelementid varchar(48) not null,
		  dependentelementtype varchar(64) not null,
		  dependencytype varchar(32) not null,
		  primary key (elementid, dependentelementid)
);
grant select, update, insert, delete on bimrl_elementdependency_&1 to public;

create index idx_elementtype_&1 on bimrl_element_&1 (elementtype);

create index idx_topofeid_&1 on bimrl_topo_face_&1 (elementid);

create index bimrl_connectingelement_&1 on bimrl_relconnection_&1 (connectingelementid);

create index bimrl_connectedelement_&1 on bimrl_relconnection_&1 (connectedelementid);

create index idx_typmaterial_id_&1 on bimrl_typematerial_&1 (elementid);

create index idx_elemmaterial_id_&1 on bimrl_elementmaterial_&1 (elementid);

create index idx_spatial_cellid_&1 on bimrl_spatialindex_&1 (cellid);

create index ixminb_spatialindex_&1 on bimrl_spatialindex_&1 (xminbound);
create index iyminb_spatialindex_&1 on bimrl_spatialindex_&1 (yminbound);
create index izminb_spatialindex_&1 on bimrl_spatialindex_&1 (zminbound);
create index ixmaxb_spatialindex_&1 on bimrl_spatialindex_&1 (xmaxbound);
create index iymaxb_spatialindex_&1 on bimrl_spatialindex_&1 (ymaxbound);
create index izmaxb_spatialindex_&1 on bimrl_spatialindex_&1 (zmaxbound);

alter table bimrl_element_&1 add constraint fk_mmodelid_&1 foreign key (modelid) references bimrl_modelinfo_&1 (modelid);

alter table bimrl_element_&1 add constraint fk_typeid_&1 foreign key (typeid) references bimrl_type_&1 (elementid);
alter table bimrl_element_&1 add constraint fk_ownerhistid_&1 foreign key (modelid, ownerhistoryid) references bimrl_ownerhistory_&1 (modelid, id);
alter table bimrl_type_&1 add constraint fk_townerhistid_&1 foreign key (modelid, ownerhistoryid) references bimrl_ownerhistory_&1 (modelid, id);

alter table bimrl_typeproperties_&1 add constraint fk_typeprop_id_&1 foreign key (elementid) references bimrl_type_&1 (elementid);

alter table bimrl_elementproperties_&1 add constraint fk_elemprop_id_&1 foreign key (elementid) references bimrl_element_&1 (elementid);

alter table bimrl_relconnection_&1 add constraint fk_connecting_&1 foreign key (connectingelementid) references bimrl_element_&1 (elementid);
alter table bimrl_relconnection_&1 add constraint fk_connected_&1 foreign key (connectedelementid) references bimrl_element_&1 (elementid);
alter table bimrl_relconnection_&1 add constraint fk_realizing_&1 foreign key (realizingelementid) references bimrl_element_&1 (elementid);

alter table bimrl_elemclassification_&1 add constraint fk_classifelemid_&1 foreign key (elementid) references bimrl_element_&1 (elementid);
alter table bimrl_elemclassification_&1 add constraint fk_classifcode_&1 foreign key (classificationname, classificationitemcode) references bimrl_classification_&1 (classificationname, classificationitemcode);
alter table bimrl_typclassification_&1 add constraint fk_classiftelemid_&1 foreign key (elementid) references bimrl_type_&1 (elementid);
alter table bimrl_typclassification_&1 add constraint fk_classiftcode_&1 foreign key (classificationname, classificationitemcode) references bimrl_classification_&1 (classificationname, classificationitemcode);

alter table bimrl_elementmaterial_&1 add constraint fk_material_eid_&1 foreign key (elementid) references bimrl_element_&1 (elementid);

alter table bimrl_typematerial_&1 add constraint fk_typematerial_tid_&1 foreign key (elementid) references bimrl_type_&1 (elementid);

alter table bimrl_relaggregation_&1 add constraint fk_master_&1 foreign key (masterelementid) references bimrl_element_&1 (elementid);
alter table bimrl_relaggregation_&1 add constraint fk_aggregate_&1 foreign key (aggregateelementid) references bimrl_element_&1 (elementid);

alter table bimrl_relspaceboundary_&1 add constraint fk_space_&1 foreign key (spaceelementid) references bimrl_element_&1 (elementid);
alter table bimrl_relspaceboundary_&1 add constraint fk_boundaries_&1 foreign key (boundaryelementid) references bimrl_element_&1 (elementid);
alter table bimrl_relspaceb_detail_&1 add constraint fk_space_det_&1 foreign key (spaceelementid) references bimrl_element_&1 (elementid);
alter table bimrl_relspaceb_detail_&1 add constraint fk_boundaries_det_&1 foreign key (boundaryelementid) references bimrl_element_&1 (elementid);

alter table bimrl_spatialstructure_&1 add constraint fk_parent_&1 foreign key (parentid) references bimrl_element_&1 (elementid);
alter table bimrl_spatialstructure_&1 add constraint fk_spatialid_&1 foreign key (spatialelementid) references bimrl_element_&1 (elementid);

alter table bimrl_relgroup_&1 add constraint fk_group_&1 foreign key (groupelementid) references bimrl_element_&1 (elementid);
alter table bimrl_relgroup_&1 add constraint fk_member_&1 foreign key (memberelementid) references bimrl_element_&1 (elementid);

alter table bimrl_elementdependency_&1 add constraint fk_dependency_&1 foreign key (elementid) references bimrl_element_&1 (elementid);
alter table bimrl_elementdependency_&1 add constraint fk_dependency_eid_&1 foreign key (dependentelementid) references bimrl_element_&1 (elementid);
