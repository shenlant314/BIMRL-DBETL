create table bimrl_relspaceb_detail_&1 (
		  spaceelementid varchar(22) not null,
		  sfaceboundid varchar(22) not null,
		  commmonpointats point3d,
		  boundaryelementid varchar(22) not null,
		  bfaceboundid varchar(22) not null,
		  commonpointatb point3d,
		  primary key (spaceelementid, sfaceboundid, boundaryelementid, bfaceboundid)
);

alter table bimrl_relspaceb_detail_&1 add constraint fk_space_det_&1 foreign key (spaceelementid) references bimrl_element_&1 (elementid);
alter table bimrl_relspaceb_detail_&1 add constraint fk_boundaries_det_&1 foreign key (boundaryelementid) references bimrl_element_&1 (elementid);

