class TraceExporter:
    def __init__(self, intake_url: str): ...
    def send(self, data: bytes, trace_count: int) -> bytes: ...
