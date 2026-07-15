class PipelineError(Exception):
    pass


class ExtractError(PipelineError):
    pass


class TransformError(PipelineError):
    pass


class LoadError(PipelineError):
    pass


class ConfigError(PipelineError):
    pass
