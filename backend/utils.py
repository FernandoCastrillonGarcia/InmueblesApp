import re
from fastembed import TextEmbedding
import uuid

def preprocess_text(text: str) -> str:
    text = text.replace('\n', ' ')
    text = re.sub(r'[\u00A0\u1680\u2000-\u200B\u202F\u205F\u3000]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

_embedding_model: TextEmbedding | None = None

def get_embedding_model() -> TextEmbedding:
    global _embedding_model
    if _embedding_model is None:
        import onnxruntime as ort
        available = ort.get_available_providers()
        providers = ["CUDAExecutionProvider", "CPUExecutionProvider"] if "CUDAExecutionProvider" in available else ["CPUExecutionProvider"]
        _embedding_model = TextEmbedding(
            model_name="nomic-ai/nomic-embed-text-v1.5",
            providers=providers,
            show_progress=False
        )
    return _embedding_model

def embed(text: list[str] | str) -> list[list[float]]:
    if isinstance(text, list):
        input_texts = [preprocess_text(t) for t in text]
    else:
        input_texts = [preprocess_text(text)]
    
    model = get_embedding_model()
    embeddings = list(model.embed(input_texts, batch_size=8))
    return [e.tolist() for e in embeddings]

def create_uuid_from_string(input_data):
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, str(input_data)))