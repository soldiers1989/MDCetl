SELECT * FROM Crunchbase.OrganizationsSummary WHERE data_fetched = 1

SELECT * FROM CRUNCHBASE.Team 
SELECT * FROM CRUNCHBASE.Person 
SELECT * FROM CRUNCHBASE.Funding_Rounds
SELECT * FROM CRUNCHBASE.Investments
SELECT * FROM CRUNCHBASE.Investors
SELECT * FROM CRUNCHBASE.Partners
SELECT * FROM Crunchbase.SubOrganization
SELECT * FROM Crunchbase.Offices
SELECT * FROM Crunchbase.Org_Category ORDER BY org_uuid
SELECT * FROM CRUNCHBASE.Founders
SELECT * FROM CRUNCHBASE.Acquisition
SELECT * FROM Crunchbase.Acquired_by
SELECT * FROM CRUNCHBASE.IPO
SELECT * FROM CRUNCHBASE.Funds
SELECT * FROM Crunchbase.Websites
SELECT * FROM Crunchbase.Image
SELECT * FROM CRUNCHBASE.News

SELECT * FROM Crunchbase.OrganizationsSummary WHERE name like 'wattpad'

-- TRUNCATE TABLE CRUNCHBASE.Funding_Rounds
-- TRUNCATE TABLE CRUNCHBASE.Team
-- TRUNCATE TABLE Crunchbase.Person
-- TRUNCATE TABLE Crunchbase.Investments
-- TRUNCATE TABLE CRUNCHBASE.Investors
-- TRUNCATE TABLE CRUNCHBASE.Partners
-- TRUNCATE TABLE Crunchbase.SubOrganization
-- TRUNCATE TABLE Crunchbase.Offices
-- TRUNCATE TABLE Crunchbase.Org_Category
-- TRUNCATE TABLE CRUNCHBASE.Founders
-- TRUNCATE TABLE CRUNCHBASE.Acquisition
-- TRUNCATE TABLE Crunchbase.Acquired_by
-- TRUNCATE TABLE Crunchbase.IPO
-- TRUNCATE TABLE Crunchbase.Websites
-- TRUNCATE TABLE Crunchbase.IMAGE
-- TRUNCATE TABLE CRUNCHBASE.News

-- UPDATE Crunchbase.OrganizationsSummary SET data_fetched = 0


