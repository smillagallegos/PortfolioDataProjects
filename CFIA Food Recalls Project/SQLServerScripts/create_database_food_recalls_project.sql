/*
    Script: create_database_food_recalls_project.sql
    Purpose: Checks for and creates the database if it doesn't already exist.
    Author: Salma Milla
    Date: 2025-05-10
    Description:
        - Used as part of the CFIA food recall data pipeline.
        - This database will store all processed recall records.
        - Ensures the pipeline can run without manual database setup.
*/

-- Check if the database exists
IF NOT EXISTS (
	SELECT name 
	FROM sys.databases 
	WHERE name = N'[Food_Recalls_DB]'
)
BEGIN
	-- Create the database if it doesn't exist
	CREATE DATABASE [Food_Recalls_DB];
	PRINT 'Database created.';
END
ELSE
BEGIN
	-- If it already exists, inform the user
	PRINT 'Database already exists.'
END
GO