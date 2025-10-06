namespace AES.Evaluator.Models;

public sealed record EssayWithRubric(
    EssayRecord Essay,
    string Rubric
)
{
    public string Id => Essay.Id;
    public string Year => Essay.Year;
    public string EssayType => Essay.EssayType;
    public string EssayContent => Essay.EssayContent;
    public int? GoldScore => Essay.GoldScore;
}
