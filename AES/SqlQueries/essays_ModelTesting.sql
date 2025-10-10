SELECT 
      ROW_NUMBER() OVER (ORDER BY ar.Year ASC) AS EssayID
      ,[Year]
      ,[EssayType]
      ,[EssayContent]
      ,ar.ReaderId
      ,StudentId
      ,Score
  FROM LH_DSP_AES.[dbo].[tblDSP_ApplicantReaderDetails] ar
  join LH_DSP_AES.[dbo].[App_tblReader] r
    on ar.ReaderId = r.ReaderId
    and r.IsSuperReader = 1
  where 
    EssayType in ('DegreeFit','CollegeChoice')
    and [Year] in (2023,2024,2025)
    and ReaderScope = 'FinalSelection'