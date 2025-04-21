USE EShop;
CREATE USER shop_reader FOR LOGIN shop_reader;

USE vietnamese_administrative_units;
CREATE USER shop_reader FOR LOGIN shop_reader;

USE Eshop;
ALTER ROLE db_datareader ADD MEMBER shop_reader;
ALTER ROLE db_datawriter ADD MEMBER shop_reader;
ALTER ROLE db_ddladmin ADD MEMBER shop_reader;

USE vietnamese_administrative_units;
ALTER ROLE db_datareader ADD MEMBER shop_reader;
ALTER ROLE db_datawriter ADD MEMBER shop_reader;
ALTER ROLE db_ddladmin ADD MEMBER shop_reader;  