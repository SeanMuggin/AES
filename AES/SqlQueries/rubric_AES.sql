WITH rubric_scores AS (
    SELECT
        appyear,
        essay,
        CONCAT(
            'Score ',
            score,
            ': ',
            STRING_AGG(
                COALESCE(STRING_ESCAPE(score_criteria, 'json'), N''),
                '; '
            ) WITHIN GROUP (ORDER BY score_criteria)
        ) AS ScoreLabel
    FROM LH_DSP_AES.dbo.dsp_rubric
    GROUP BY appyear, essay, score
),
rubric_summary AS (
    SELECT
        appyear AS [Year],
        essay AS EssayType,
        STRING_AGG(ScoreLabel, '. ') AS Rubric
    FROM rubric_scores
    GROUP BY appyear, essay
),
essay_questions AS (
    SELECT
        [Year],
        EssayType,
        CONCAT('Essay Question: ', Prompt) AS EssayQuestion
    FROM [LH_DSP_AES].[dbo].[essay_questions]
)
SELECT
    2025 [Year],
    rs.EssayType,
    CONCAT(eq.EssayQuestion, ' ', rs.Rubric) AS Rubric
FROM rubric_summary rs
JOIN essay_questions eq
    ON rs.[Year] = eq.[Year]
   AND rs.EssayType = eq.EssayType
   and rs.[Year] = 2025