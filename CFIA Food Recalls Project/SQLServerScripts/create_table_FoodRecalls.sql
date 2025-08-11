/* 
    Script: create_table_FoodRecalls.sql
    Purpose: Creates the FoodRecalls table in the CFIA_Recalls database if it does not exist.
    Author: Salma Milla
    Date: 2025-05-10
    Description:
        - This table stores data about Canadian food recalls.
        - Includes classification, issue type, product and company information.
        - Designed for ingestion from the cleaned CFIA data pipeline.
*/

USE CFIA_Recalls;
GO

-- Create table if it does not already exist
IF NOT EXISTS (
	SELECT *
	FROM sys.tables
	WHERE name = 'FoodRecalls' AND type = 'U'
)
BEGIN
	CREATE TABLE dbo.FoodRecalls (
		ID INT IDENTITY(1,1) PRIMARY KEY,
		NID INT NOT NULL,
		Title VARCHAR (500) NOT NULL,
		[URL] VARCHAR (500),
		[Product] VARCHAR (255) NOT NULL,
		Issue VARCHAR (255) NOT NULL,
		MainIssue VARCHAR (255),
		SecondaryIssue VARCHAR (255),
		BacteriaSubtype VARCHAR (255),
		Category VARCHAR (100) NOT NULL,
		Class VARCHAR(10) NOT NULL,
		LastUpdated DATE NOT NULL,
		IsArchived BIT DEFAULT 0 NOT NULL,
		CreatedAt DATETIME DEFAULT GETDATE(),

		CONSTRAINT UQ_FoodRecalls_NID UNIQUE (NID),
)
END
GO

-- Get db content
SELECT * FROM dbo.FoodRecalls;