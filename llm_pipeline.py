import pymupdf
import random
import json
from pathlib import Path
from openai import OpenAI
from dataclasses import dataclass
from clients import XAIClient
from dotenv import load_dotenv
import os

load_dotenv()

PDF_PATH = "doc.pdf"
OUTPUT_PATH = "training_data.jsonl"
CHUNK_SIZE = 800
MODEL = "grok-3-mini"


client = XAIClient()


def chunk_text(text, chunk_size=CHUNK_SIZE):
    words = text.split()
    for i in range(0, len(words), chunk_size):
        yield " ".join(words[i : i + chunk_size])


def make_summary_example(chunk, level="short"):
    if level == "short":
        max_tokens = 50
    elif level == "medium":
        max_tokens = 150
    else:
        max_tokens = 400

    prompt = f"""
You are preparing training data for instruction tuning.
Task: Summarize the given text.

Constraints:
- Focus on names, dates, claims, and key facts.
- Length must not exceed {max_tokens} tokens.
- Do not invent information not in the text.
- Output only the summary, no explanations.

Text:
<<<
{chunk}
>>>

Summary:
"""
    summary = client.get_response(model=MODEL, messages=prompt)
    return {
        "instruction": f"Summarize the following text in under {max_tokens} tokens, focusing on names, dates, and key facts:",
        "input": chunk,
        "output": summary
    }


def make_extractive_qa_example(chunk):
    prompt = f"""
You are preparing extractive Q/A training data for instruction tuning.
From the text below, create one question that can be answered with a short direct quote.

Constraints:
- The question should ask about a fact explicitly stated in the text.
- The answer must be a direct quote from the text, ≤40 tokens.
- Output must be in strict JSON with keys: {{"instruction": ..., "input": ..., "output": ...}}

Text:
<<<
{chunk}
>>>

JSON:
"""
    result = client.get_response(model=MODEL, messages=prompt)
    try:
        return json.loads(result)
    except Exception:
        return None


def make_negative_example(chunk):
    prompt = f"""
You are preparing training data for refusal behavior.
Task: Ask a question that cannot be answered from the given text.

Constraints:
- Use a generic factual question unrelated to the text (e.g., \"What is the capital of France?\").
- The answer must be exactly: \"Insufficient information in record.\".
- Output must be in strict JSON with keys: {{"instruction": ..., "input": ..., "output": ...}}

Text:
<<<
{chunk}
>>>

JSON:
"""
    result = client.get_response(model=MODEL, messages=prompt)
    try:
        return json.loads(result)
    except Exception:
        return None


def build_dataset(pdf_path=PDF_PATH, output_path=OUTPUT_PATH, max_chunks=20):
    doc = pymupdf.open(pdf_path)
    text = "\n".join([page.get_text("text") for page in doc])
    chunks = list(chunk_text(text))

    dataset = []
    for chunk in chunks[:max_chunks]:
        dataset.append(make_summary_example(chunk, "short"))
        dataset.append(make_summary_example(chunk, "medium"))

        if random.random() < 0.2:
            dataset.append(make_summary_example(chunk, "long"))

        qa = make_extractive_qa_example(chunk)
        if qa:
            dataset.append(qa)

        if random.random() < 0.1:
            neg = make_negative_example(chunk)
            if neg:
                dataset.append(neg)

    with open(output_path, "w", encoding="utf-8") as f:
        for ex in dataset:
            f.write(json.dumps(ex, ensure_ascii=False) + "\n")

    print(f"Dataset written to {output_path}, {len(dataset)} samples")


if __name__ == "__main__":
    build_dataset()
