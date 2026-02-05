from enum import Enum


class TaskType(Enum):
    ALERT = "alert"
    PRE_EVENT = "pre_event"
    POST_EVENT = "post_event"
    DIVERGENCE = "divergence"
    TERM = "term"
    GENERIC = "generic"


class LLMRouter:
    """
    Professional model router for OpenRouter.
    Decides WHICH model should handle each task.
    """

    def __init__(self):
        # Models are injected via environment variables
        import os

        self.analyst = os.getenv("OPENROUTER_MODEL_ANALYST")
        self.explainer = os.getenv("OPENROUTER_MODEL_EXPLAINER")
        self.generalist = os.getenv("OPENROUTER_MODEL_GENERALIST")

        if not self.analyst:
            raise ValueError("OPENROUTER_MODEL_ANALYST not set")

        if not self.generalist:
            raise ValueError("OPENROUTER_MODEL_GENERALIST not set")

    def route(self, task: TaskType) -> str:
        """
        Returns the best model for a given task.
        """

        if task in [
            TaskType.ALERT,
            TaskType.PRE_EVENT,
            TaskType.POST_EVENT,
            TaskType.DIVERGENCE,
        ]:
            return self.analyst

        if task == TaskType.TERM:
            return self.generalist

        return self.generalist