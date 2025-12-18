-- =============================================================================
-- FINANCIAL FIGURES TABLE
-- =============================================================================
USE mckinsey;
CREATE TABLE `Financial Figures` (
	`Financial Figures ID` INT NOT NULL AUTO_INCREMENT, -- PRIMARY KEY
	`Units` INT,
	`Sales` FLOAT,
	`Cost` FLOAT,
	`Gross Profit` FLOAT,
	`Order ID` INT, -- FK
	`Factory ID` CHAR(100), -- FK
	`Product ID` CHAR(100),  -- FK
	PRIMARY KEY (`Financial Figures ID`)
);

-- ---------------------------------------------------------------------------------------------------

-- empty table 
SELECT * FROM `Financial Figures`;

-- INSERT DATA INTO THE TABLE----------------------------------------------------------
INSERT INTO `Financial Figures` ( `Units`, `Sales`, `Cost`, `Gross Profit`, `Order ID`)
SELECT
	`Units`,
    `Sales`,
	`Cost`,
	`Gross Profit`
FROM wonka_choc_factory;

SELECT `Order ID` FROM `Order`

GROUP BY
	`Units`,
	`Sales`,
	`Cost`,
	`Gross Profit`,
    `Order ID`;
    
-- full table 
SELECT * FROM `Financial Figures`;
-- ---------------------------------------------------------------------------
INSERT INTO `Financial Figures` ( `Order ID`)
SELECT
	`Order ID`
FROM `Order`
GROUP BY
	`Order ID`;

-- add FK Product ID ---------------------------------------------
ALTER TABLE `Financial Figures` 
ADD CONSTRAINT `order figures`
FOREIGN KEY (`Order ID`) REFERENCES `Order` (`Order ID`);

-- add FK Product ID ---------------------------------------------
ALTER TABLE `Financial Figures` 
ADD CONSTRAINT `product figures`
FOREIGN KEY (`Product ID`) REFERENCES `Product` (`Product ID`);

-- add FK Factory ID ---------------------------------------------
ALTER TABLE `Financial Figures`
ADD CONSTRAINT `factory figures`
FOREIGN KEY (`Factory ID`) REFERENCES `Factory` (`Factory ID`);

-- full table 
SELECT * FROM `Financial Figures`;



-- =============================================================================
-- CUSTOMER TABLE
-- =============================================================================
USE mckinsey;
CREATE TABLE `Customer` (
  `Customer ID` INT,   -- primary key
  `Country/Region`  CHAR(100),
  `State/Province`  CHAR(100),
  `City`  CHAR(100),
  `Postal Code`   INT,
  PRIMARY KEY (`Customer ID`)
);

-- insert data in Customer
INSERT INTO `Customer` (`Customer ID`, `Country/Region`, `State/Province`,`City`,`Postal Code`)
SELECT DISTINCT
		`Customer ID` AS `Customer ID`,   -- primary key
		`Country/Region`,
		`State/Province`,
		`City`,
		`Postal Code`
FROM `wonka_choc_factory`;

SELECT * FROM `Customer`;
-- =============================================================================
-- ORDER TABLE
-- =============================================================================
CREATE TABLE `Order` (
  `Order ID` INT NOT NULL AUTO_INCREMENT, -- Primary Key
  `Customer ID` INT, -- FK
  `Order Date` DATE,
  `Ship Date` DATE,
  `Ship Mode` CHAR(100),
  PRIMARY KEY (`Order ID`)
);
-- insert data
INSERT INTO `Order` (`Customer ID`, `Order Date`, `Ship Date`, `Ship Mode`)
SELECT
  `Customer ID`, -- FK Customer ID
  STR_TO_DATE(`Order Date`, '%Y-%m-%d'),
  STR_TO_DATE(`Ship Date`, '%Y-%m-%d'),
  `Ship Mode`
FROM wonka_choc_factory
GROUP BY
  `Customer ID`,
  `Order Date`,
  `Ship Date`,
  `Ship Mode`;
-- -------------------------------------------------------------------------------
-- CONSTRAINT CUSTOMER ORDER
ALTER TABLE `Order`
    ADD CONSTRAINT `customer_order_relationship` 
		FOREIGN KEY (`Customer ID`) 
        REFERENCES `Customer`(`Customer ID`);

-- print table order
SELECT * FROM `Order`;
-- =============================================================================
-- FACTORY TABLE
-- =============================================================================
USE mckinsey;
CREATE TABLE `Factory` (
  `Factory ID` CHAR(50) NOT NULL,   -- primary key
  `Longitude`  FLOAT,
  `Latitude`   FLOAT,
  PRIMARY KEY (`Factory ID`)
);

INSERT INTO `Factory` (`Factory ID`, `Longitude`, `Latitude`)
SELECT DISTINCT
       `Factory`   AS `Factory ID`,
       `Longitude`,
       `Latitude`
FROM `wonka_choc_factory`;

SELECT * FROM `Factory`;
-- =============================================================================
-- PRODUCT TABLE
-- =============================================================================
CREATE TABLE `Product`(
	`Product ID` CHAR(50), -- primary key
    `Factory ID` CHAR(50), -- foreign key from factory
    `Product Name` VARCHAR(50),
    `Division`VARCHAR(50),
    PRIMARY KEY (`Product ID`)
);
-- print 
SELECT * FROM `Product`;
-- 
USE mckinsey;
INSERT INTO `Product` (`Product ID`, `Factory ID`, `Product Name`, `Division`)
SELECT *
FROM (
  SELECT DISTINCT
    CASE `Product Name`
      WHEN 'Wonka Bar'                         THEN 'WB'
      WHEN 'Wonka Bar - Triple Dazzle Caramel' THEN 'WBTC'
      WHEN 'Wonka Bar - Scrumdiddlyumptious'   THEN 'WBSL'
      WHEN 'Wonka Bar - Fudge Mallows'         THEN 'WBFM'
      WHEN 'Wonka Bar - Milk Chocolate'        THEN 'WBMC'
      WHEN 'Wonka Bar - Nutty Crunch Surprise' THEN 'WBNS'
      WHEN 'Wonka Gum'                         THEN 'WG'
      WHEN 'SweeTARTS'                         THEN 'ST'
      WHEN 'Lickable Wallpaper'                THEN 'LW'
      WHEN 'Kazookles'                         THEN 'KK'
      WHEN 'Everlasting Gobstopper'            THEN 'EG'
      WHEN 'Fizzly Lifting Drinks'             THEN 'FLD'
      WHEN 'Nerds'                             THEN 'ND'
      WHEN 'Fun Dip'                           THEN 'FD'
      WHEN 'Laffy-Taffy'                       THEN 'LT'
      WHEN 'Hair Toffee'                       THEN 'HT'
      ELSE NULL
    END       AS `Product ID`,
    `Factory` AS `Factory ID`,
    `Product Name`,
    `Division`
  FROM `wonka_choc_factory`
) AS t
WHERE `Product ID` IS NOT NULL;

SELECT * FROM `Product`;

-- CONSTRAINT 
ALTER TABLE `Product`
    ADD CONSTRAINT `product_factory_relationship` 
		FOREIGN KEY (`Factory ID`) 
        REFERENCES `Factory`(`Factory ID`);
        
-- =============================================================================
-- PRODUCT PROFITABILITY AND MARGIN
-- =============================================================================
  
-- PROFITABILITY TABLE BY PRODUCT  
SELECT
    p.`Product ID`,
    p.`Product Name`,
    SUM(ff.`Sales`)        AS total_sales,
    SUM(ff.`Cost`)         AS total_cost,
    SUM(ff.`Gross Profit`) AS total_gross_profit,
    SUM(ff.`Gross Profit`) / SUM(ff.`Sales`)  AS profit_margin,
    SUM(ff.`Gross Profit`) / SUM(ff.`Units`)  AS profit_per_unit
FROM `Financial Figures` ff
JOIN `Product` p
  ON ff.`Product ID` = p.`Product ID`
GROUP BY
    p.`Product ID`,
    p.`Product Name`
ORDER BY
    profit_margin DESC;

-- BEST PRODUCTS BY PROFIT MARGIN
SELECT *
FROM (
    SELECT
        p.`Product ID`,
        p.`Product Name`,
        SUM(ff.`Sales`)        AS total_sales,
        SUM(ff.`Gross Profit`) AS total_gross_profit,
        SUM(ff.`Gross Profit`) / SUM(ff.`Sales`) AS profit_margin
    FROM `Financial Figures` ff
    JOIN `Product` p
      ON ff.`Product ID` = p.`Product ID`
    GROUP BY p.`Product ID`, p.`Product Name`
) t
ORDER BY profit_margin DESC
LIMIT 5;


-- WORST PRODUCT BY PROFIT MARGIN
SELECT *
FROM (
    SELECT
        p.`Product ID`,
        p.`Product Name`,
        SUM(ff.`Sales`)        AS total_sales,
        SUM(ff.`Gross Profit`) AS total_gross_profit,
        SUM(ff.`Gross Profit`) / SUM(ff.`Sales`) AS profit_margin
    FROM `Financial Figures` ff
    JOIN `Product` p
      ON ff.`Product ID` = p.`Product ID`
    GROUP BY p.`Product ID`, p.`Product Name`
) t
ORDER BY profit_margin ASC
LIMIT 5;