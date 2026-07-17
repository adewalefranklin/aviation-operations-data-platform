class PipelineError(Exception):
    pass


class ConfigError(PipelineError):
    pass


class AuthenticationError(PipelineError):
    pass


class ExtractError(PipelineError):
    pass


class TransformError(PipelineError):
    pass


class LoadError(PipelineError):
    pass
