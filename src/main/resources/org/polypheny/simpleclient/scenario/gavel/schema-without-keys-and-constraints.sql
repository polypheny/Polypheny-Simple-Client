CREATE TABLE "user" ( "id" integer NOT NULL, "email" varchar(100) NOT NULL, "password" varchar(100) NOT NULL, "last_name" varchar(50) NOT NULL, "first_name" varchar(50) NOT NULL, "gender" varchar(1) NOT NULL, "birthday" date NOT NULL, "city" varchar(50) NOT NULL, "zip_code" varchar(20) NOT NULL, "country" varchar(50) NOT NULL )
CREATE TABLE "bid" ( "id" integer NOT NULL, "amount" integer NOT NULL, "timestamp" timestamp  NOT NULL, "user" integer NOT NULL, "auction" integer NOT NULL )
CREATE TABLE "picture" ( "filename" varchar(50) NOT NULL, "type" varchar(20) NOT NULL, "size" integer NOT NULL, "auction" integer NOT NULL )
CREATE TABLE "auction" ( "id" integer NOT NULL, "title" varchar(100) NOT NULL, "description" varchar(2500) NOT NULL, "start_date" timestamp  NOT NULL, "end_date" timestamp  NOT NULL, "category" integer NOT NULL, "user" integer NOT NULL )
CREATE TABLE "category" ( "id" integer NOT NULL, "name" varchar(100) NOT NULL )
