CREATE DATABASE company;

USE company;

-- ----------------------------------------------------------
-- NIVELL 1
-- ----------------------------------------------------------
-- Creem les taules
CREATE TABLE transactions ( -- Pas imprescindible en Mac per reconèixer les columnes del csv correctament.
    id VARCHAR(50) PRIMARY KEY, -- No fa falta NOT NULL, una primay key mai es nula.
    card_id VARCHAR(20),
    business_id VARCHAR(20),
    timestamp TIMESTAMP,
    amount DECIMAL(10,2),
    declined TINYINT,
    product_ids VARCHAR(50),
    user_id INT,
    lat FLOAT,
    longitude FLOAT
);

CREATE TABLE credit_cards (
	id VARCHAR(20) PRiMARY KEY,
    user_id INT,
    iban VARCHAR(50),
    pan VARCHAR(50),
    pin VARCHAR(10),
    cvv VARCHAR(10), -- Millor fer servir varchar, un int eliminaria qualsevol 0 per davant.
    track1 VARCHAR(100),
    track2 VARCHAR(100),
    expiring_date DATE -- Millor utilitzar DATE que no pas VARCHAR per fer consultes de dates.
);

CREATE TABLE companies (
	company_id VARCHAR(15) PRIMARY KEY,
    company_name VARCHAR(50),
    phone VARCHAR(15),
    email VARCHAR(50),
    country VARCHAR(20),
    website VARCHAR(100)
);

CREATE TABLE users (
	id INT PRIMARY KEY,
    name VARCHAR(20),
    surname VARCHAR(20),
    phone VARCHAR(15),
    email VARCHAR(50),
    birth_date DATE,
    country VARCHAR(20),
    city VARCHAR(20),
    postal_code VARCHAR(15),
    address VARCHAR(50),
    continent VARCHAR(15)
);

CREATE TABLE products (
	id INT PRIMARY KEY,
    product_name VARCHAR(50),
    price DECIMAL(10,2),
    colour CHAR(7), -- codis de colors hexadecimals (longitud fixa  - CHAR)
    weight DECIMAL(10,2),
    warehouse_id VARCHAR(10)
);

-- Abans de carregar dades es important que local_infile sigui actiu.
SHOW VARIABLES LIKE 'local_infile';

SET GLOBAL local_infile = 1; -- Si esta en OFF cal aplicar aquest SET.

-- Carreguem els csv a les respectives taules
LOAD DATA LOCAL INFILE '/Users/noeliasolano/Downloads/transactionsOK.csv'
INTO TABLE transactions
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/Users/noeliasolano/Downloads/credit_cards.csv'
INTO TABLE credit_cards
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(id, user_id, iban, pan, pin, cvv, track1, track2, @expiring_date) -- Cal especificar totes les columnes entre parentesis, és el ordre que segueix mysql de lectura de dades i amb l'arroba es crea una variable temporal per fer modificacions. 
SET expiring_date = STR_TO_DATE(@expiring_date, '%m/%d/%y');

LOAD DATA LOCAL INFILE '/Users/noeliasolano/Downloads/companies.csv'
INTO TABLE companies
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/Users/noeliasolano/Downloads/american_users.csv'
INTO TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"' -- La data de naixement esta entre cometes.
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(id, name, surname, phone, email, @birth_date, country, city, postal_code, address)
SET
	birth_date = STR_TO_DATE(@birth_date, '%b %d, %Y'),
    continent = 'America';

LOAD DATA LOCAL INFILE '/Users/noeliasolano/Downloads/european_users.csv'
INTO TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(id, name, surname, phone, email, @birth_date, country, city, postal_code, address)
SET
	birth_date = STR_TO_DATE(@birth_date, '%b %d, %Y'),
	continent = 'Europe';

LOAD DATA LOCAL INFILE '/Users/noeliasolano/Downloads/products.csv'
INTO TABLE products
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(id, product_name, @price, colour, weight, warehouse_id)
SET price = REPLACE(@price, '$', '');

-- Correccions de les taules
ALTER TABLE products
RENAME COLUMN price TO price_usd;

ALTER TABLE companies
RENAME COLUMN company_id TO id;

ALTER TABLE transactions
RENAME COLUMN business_id TO company_id;

UPDATE credit_cards
SET pan = REPLACE(pan, ' ', ''); -- Per si de cas treiem els espais en blanc

-- Relacionem taules
ALTER TABLE transactions
ADD CONSTRAINT fk_transactions_users FOREIGN KEY(user_id) REFERENCES users(id);

ALTER TABLE transactions
ADD CONSTRAINT fk_transactions_credit_cards FOREIGN KEY(card_id) REFERENCES credit_cards(id);

ALTER TABLE transactions
ADD CONSTRAINT fk_transactions_companies FOREIGN KEY(company_id) REFERENCES companies(id);

-- ----------------------------------------------------------
-- Exercici 1: Mostra usuaris amb més de 80 transaccions utilitzant almenys 2 taules. Fes una subconsulta
-- ----------------------------------------------------------
SELECT *
FROM users u
WHERE EXISTS(
    SELECT 1
    FROM transactions t
	WHERE t.user_id = u.id AND declined=0
    GROUP BY t.user_id
    HAVING COUNT(t.id) > 80
);

-- ----------------------------------------------------------
-- Exercici 2: Mostra la mitjana d'amount per IBAN de les targetes de crèdit a la companyia Donec Ltd, utilitza almenys 2 taules.
-- ----------------------------------------------------------
SELECT c.company_name, cc.iban, AVG(t.amount) AS amount
FROM transactions t
JOIN companies c On c.id=t.company_id
JOIN credit_cards cc ON cc.id=t.card_id
WHERE c.company_name='Donec Ltd' AND declined=0
GROUP BY c.id, cc.iban;

-- ----------------------------------------------------------
-- NIVELL 2
-- Nova taula d'estat de les targetes: si les tres últimes transaccions han estat declinades = inactiu, si almenys una no és rebutjada = actiu
-- ----------------------------------------------------------
-- Utilitzem una window function: ROW_UMBER() que ordena i numera, es com un top n
CREATE TABLE status_cards AS
SELECT
	x.card_id,
		CASE
			WHEN SUM(declined) = 3
			THEN 'inactive'
			ELSE 'active'
		END AS status -- Hem creat un sumatori
FROM(
	SELECT t.*,
	ROW_NUMBER() OVER (PARTITION BY t.card_id ORDER BY t.timestamp DESC) AS rn -- Fem ús de timestamp perquè olem les 3 ultimes
    FROM transactions t
) x
WHERE rn <= 3 -- La columna temporal rn compta fins a 3
GROUP BY x.card_id;

-- Relacionem la taula
ALTER TABLE status_cards
ADD CONSTRAINT pk_status_cards PRIMARY KEY (card_id);

ALTER TABLE status_cards
ADD CONSTRAINT fk_status_cards_credit_cards FOREIGN KEY (card_id) REFERENCES credit_cards(id);
-- ----------------------------------------------------------
-- Exercici 1: Quantes targetes estan actives?
-- ----------------------------------------------------------
SELECT COUNT(sc.status)
FROM status_cards sc
WHERE sc.status = 'active';

-- ----------------------------------------------------------
-- NIVELL 3
-- Crea una taula amb la qual puguem unir les dades del nou arxiu products.csv amb la base de dades creada, tenint en compte que des de transaction tens product_ids.
-- ----------------------------------------------------------
-- Product_ids passa de VARCHAR a ser tipus JSON
UPDATE transactions
SET product_ids = CONCAT('[', REPLACE(product_ids,' ',''), ']') -- Treiem els espais
LIMIT 99999999;

-- Creació de la taula
CREATE TABLE transactions_products AS
SELECT t.id AS transaction_id, jt.product_id
FROM transactions t
JOIN JSON_TABLE( -- Taula temporal
	t.product_ids,
    '$[*]' COLUMNS(
		product_id INT PATH '$' -- Definició del nom i tipus
    )
) AS jt
ORDER BY transaction_id;

-- Relacionem la taula
ALTER TABLE transactions_products
ADD CONSTRAINT fk_tp_transactions FOREIGN KEY (transaction_id) REFERENCES transactions(id);

ALTER TABLE transactions_products
ADD CONSTRAINT fk_tp_products FOREIGN KEY (product_id) REFERENCES products(id);

ALTER TABLE transactions_products
ADD PRIMARY KEY (transaction_id, product_id);

-- ----------------------------------------------------------
-- Exercici 1: Necessitem conèixer el nombre de vegades que s'ha venut cada producte.
-- ----------------------------------------------------------
SELECT ts.product_id, COUNT(ts.transaction_id) AS Sales
FROM transactions_products ts
GROUP BY ts.product_id
ORDER BY product_id ASC;