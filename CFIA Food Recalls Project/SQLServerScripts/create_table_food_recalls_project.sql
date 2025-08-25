/* 
    Script: create_table_food_recalls_project.sql
    Purpose: Creates the table in the database if it doesn't exist.
    Author: Salma Milla
    Date: 2025-05-10
    Description:
        - This table stores data about Canadian food recalls.
        - Includes classification, issue type, product and company information.
        - Designed for ingestion from the cleaned CFIA data pipeline.
*/

USE [Food_Recalls_DB];
GO

-- Create table if it does not already exist
IF NOT EXISTS (
	SELECT *
	FROM sys.tables
	WHERE name = [Food_Recalls_Table] AND type = 'U'
)
BEGIN
	CREATE TABLE dbo.[Food_Recalls_Table] (
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
		CreatedAt DATETIME DEFAULT GETDATE()
)
END
GO

-- Get db content
SELECT * FROM dbo.[Food_Recalls_Table];