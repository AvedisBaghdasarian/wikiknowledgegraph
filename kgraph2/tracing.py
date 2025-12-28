from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

def setup_tracing(service_name: str = "kgraph-pipeline"):
    resource = Resource(attributes={
        SERVICE_NAME: service_name
    })

    provider = TracerProvider(resource=resource)
    
    # Default OTLP exporter points to localhost:4317 (Tempo)
    processor = BatchSpanProcessor(OTLPSpanExporter())
    provider.add_span_processor(processor)
    
    trace.set_tracer_provider(provider)

def instrument_span(span, operation_type: str = None):
    """Adds standard attributes to a span for better metrics."""
    if operation_type:
        span.set_attribute("operation.type", operation_type)
    # service.name and span.name are usually handled by the SDK/Generator by default

def get_tracer(name: str):
    return trace.get_tracer(name)
