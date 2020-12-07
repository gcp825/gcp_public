CREATE OR REPLACE PROCEDURE `your-project.skating.load_d_aspect`() 

BEGIN
  
  TRUNCATE TABLE `your-project.skating.d_aspect`;
  
  INSERT INTO `your-project.skating.d_aspect`
  SELECT * FROM `your-project.skating.s_aspect_v`;
  
-- COMMIT; -- This raises an error as no active tx to commit; BQ must be committing implicitly with each step
  
END;
