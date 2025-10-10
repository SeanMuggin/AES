-- Using history for testing because tblEssay is null until new application cycle begins
-- will need to remove 'top 100' and ScholarYear filter when moving to dbo.tblEssay
SELECT top 100
                        [EssayID],
                        2025 [Year],
                        case
                                when EssayType like '%college choices' then 'CollegeChoice'
                                when EssayType like '%major choice and post graduation and career goals' then 'DegreeFit'
                                end [EssayType],
                        [EssayContent],
                        cast(999 as int) ReaderId,
                        [StudentID]
FROM [LH_DSP_AES].[dbo].[tblEssay_History]
where
        --EssayType in ('DegreeFit','CollegeChoice')
        (EssayType like '%college choices' or EssayType like '%major choice and post graduation and career goals')
        and ScholarYear = 2025
