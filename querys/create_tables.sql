CREATE TABLE dbo.component_type ( 
	id                   int NOT NULL   IDENTITY,
	component_type_id    varchar(25)    ,
	component_type       varchar(50)    ,
	CONSTRAINT Pk_component_type_id PRIMARY KEY ( id )
 );

CREATE TABLE dbo.connection_type ( 
	id                   int NOT NULL   IDENTITY,
	connection_type_id   varchar(25)    ,
	CONSTRAINT Pk_connection_type_id PRIMARY KEY ( id )
 );

CREATE TABLE dbo.stg_component ( 
	component_id         varchar(500)    ,
	component_type_id    varchar(500)    ,
	type                 varchar(500)    ,
	connection_type_id   varchar(500)    ,
	outside_shape        varchar(500)    ,
	base_type            varchar(500)    ,
	height_over_tube     varchar(500)    ,
	bolt_pattern_long    varchar(500)    ,
	bolt_pattern_wide    varchar(500)    ,
	groove               varchar(500)    ,
	base_diameter        varchar(500)    ,
	shoulder_diameter    varchar(500)    ,
	unique_feature       varchar(500)    ,
	orientation          varchar(500)    ,
	weight               varchar(500)    
 );

CREATE TABLE dbo.stg_materials ( 
	tube_assembly_id     varchar(500)    ,
	component_id_1       varchar(500)    ,
	quantity_1           varchar(500)    ,
	component_id_2       varchar(500)    ,
	quantity_2           varchar(500)    ,
	component_id_3       varchar(500)    ,
	quantity_3           varchar(500)    ,
	component_id_4       varchar(500)    ,
	quantity_4           varchar(500)    ,
	component_id_5       varchar(500)    ,
	quantity_5           varchar(500)    ,
	component_id_6       varchar(500)    ,
	quantity_6           varchar(500)    ,
	component_id_7       varchar(500)    ,
	quantity_7           varchar(500)    ,
	component_id_8       varchar(500)    ,
	quantity_8           varchar(500)    
 );

CREATE TABLE dbo.stg_price_quote ( 
	tube_assembly_id     varchar(500)    ,
	supplier             varchar(500)    ,
	quote_date           varchar(500)    ,
	annual_usage         varchar(500)    ,
	min_order_quantity   varchar(500)    ,
	bracket_pricing      varchar(500)    ,
	quantity             varchar(500)    ,
	cost                 varchar(500)    
 );

CREATE TABLE dbo.supplier ( 
	id                   int NOT NULL   IDENTITY,
	supplier             varchar(25)    ,
	CONSTRAINT Pk_supplier_id PRIMARY KEY ( id )
 );

CREATE TABLE dbo.tube_assembly ( 
	id                   int NOT NULL   IDENTITY,
	tube_assembly_id     varchar(25)    ,
	CONSTRAINT Pk_tube_assembly_id PRIMARY KEY ( id )
 );

CREATE TABLE dbo.component ( 
	id                   int NOT NULL   IDENTITY,
	component_id         varchar(50)    ,
	component_type_id    int    ,
	connection_type_id   int    ,
	outside_shape        varchar(30)    ,
	height_over_tube     numeric(8,1)    ,
	bolt_pattern_long    numeric(8,1)    ,
	bolt_pattern_wide    numeric(8,1)    ,
	groove               varchar(5)    ,
	base_type            varchar(100)    ,
	base_diameter        numeric(8,1)    ,
	shoulder_diameter    numeric(8,1)    ,
	unique_feature       varchar(5)    ,
	orientation          varchar(5)    ,
	weight               numeric(16,4)    ,
	CONSTRAINT Pk_comp_boss_id PRIMARY KEY ( id ),
	CONSTRAINT Unq_comp_boss_connection_type_id UNIQUE ( component_id ) 
 );

CREATE  INDEX Idx_component_component_type_id ON dbo.component ( component_type_id );

CREATE TABLE dbo.materials ( 
	id                   int NOT NULL   IDENTITY,
	tube_assembly_id     int    ,
	component_id         int    ,
	quantity             int    ,
	CONSTRAINT Pk_materials_id PRIMARY KEY ( id ),
	CONSTRAINT Unq_assembly_component UNIQUE ( tube_assembly_id, component_id ) 
 );

CREATE TABLE dbo.price_quote ( 
	id                   int NOT NULL   IDENTITY,
	id_materials         int    ,
	id_supplier          int    ,
	quote_date           date    ,
	annual_usage         int    ,
	min_order_quantity   int    ,
	bracket_pricing      varchar(5)    ,
	quantity             int    ,
	cost                 numeric(26,18)    ,
	CONSTRAINT Pk_price_quote_id PRIMARY KEY ( id ),
	CONSTRAINT Unq_price_quote_id_materials UNIQUE ( id_materials, id_supplier, quote_date, annual_usage, min_order_quantity, bracket_pricing, quantity, cost ) 
 );

ALTER TABLE dbo.component ADD CONSTRAINT fk_component_connection_type FOREIGN KEY ( connection_type_id ) REFERENCES dbo.connection_type( id ) ON DELETE NO ACTION ON UPDATE NO ACTION;

ALTER TABLE dbo.component ADD CONSTRAINT fk_component_component_type FOREIGN KEY ( component_type_id ) REFERENCES dbo.component_type( id ) ON DELETE NO ACTION ON UPDATE NO ACTION;

ALTER TABLE dbo.materials ADD CONSTRAINT fk_materials_component FOREIGN KEY ( component_id ) REFERENCES dbo.component( id ) ON DELETE NO ACTION ON UPDATE NO ACTION;

ALTER TABLE dbo.materials ADD CONSTRAINT fk_materials_tube_assembly FOREIGN KEY ( tube_assembly_id ) REFERENCES dbo.tube_assembly( id ) ON DELETE NO ACTION ON UPDATE NO ACTION;

ALTER TABLE dbo.price_quote ADD CONSTRAINT fk_price_quote_materials FOREIGN KEY ( id_materials ) REFERENCES dbo.materials( id ) ON DELETE NO ACTION ON UPDATE NO ACTION;

ALTER TABLE dbo.price_quote ADD CONSTRAINT fk_price_quote_supplier FOREIGN KEY ( id_supplier ) REFERENCES dbo.supplier( id ) ON DELETE NO ACTION ON UPDATE NO ACTION;

