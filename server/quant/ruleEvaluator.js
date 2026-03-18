const operators = {
  gt: (l, r) => l > r,
  lt: (l, r) => l < r,
  gte: (l, r) => l >= r,
  lte: (l, r) => l <= r,
  eq: (l, r) => l === r
};

export class RuleEvaluator {
  evaluateBlock(block, context) {
    const wrapper = block.all ? 'all' : 'any';
    const conditions = block[wrapper] || [];
    const evaluated = conditions.map((condition) => this.evaluateCondition(condition, context));
    return wrapper === 'all' ? evaluated.every(Boolean) : evaluated.some(Boolean);
  }

  evaluateCondition(condition, context) {
    if (condition.type) return Boolean(context.builtin?.[condition.type]);
    const left = context.values[condition.left];
    const right = context.values[condition.right];
    if (!Number.isFinite(left) || !Number.isFinite(right)) return false;
    return operators[condition.operator]?.(left, right) || false;
  }
}
