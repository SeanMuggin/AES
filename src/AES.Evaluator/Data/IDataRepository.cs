using AES.Evaluator.Models;

namespace AES.Evaluator.Data;

public interface IDataRepository
{
    Task<IReadOnlyList<RubricRecord>> GetRubricsAsync(CancellationToken cancellationToken);
    Task<IReadOnlyList<EssayRecord>> GetEssaysAsync(CancellationToken cancellationToken);
}
