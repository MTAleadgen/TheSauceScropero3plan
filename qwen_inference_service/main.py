import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, GPTQConfig, TextIteratorStreamer
from fastapi import FastAPI, HTTPException, Body, Depends, Header
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import asyncio
import time
import os
import json # For robust JSON parsing

# --- Configuration --- 
MODEL_NAME = os.getenv("MODEL_NAME", "glide-the/Qwen3-32B-GPTQ-4bits")
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
LLM_PRECISION = os.getenv("LLM_PRECISION", "4bit") # For future quant-swap
MAX_REQUEST_BODY_SIZE = int(os.getenv("MAX_REQUEST_BODY_SIZE", 2 * 1024 * 1024)) # 2MB
EXPECTED_BEARER_TOKEN = os.getenv("API_BEARER_TOKEN", "your-secret-token") # Simple bearer token

# --- Global Model and Tokenizer --- 
tokenizer = None
model = None

app = FastAPI(title="Qwen3 Inference Service")

# --- Security Dependency --- 
async def verify_token(x_token: str = Header(None)):
    if EXPECTED_BEARER_TOKEN and (not x_token or x_token != f"Bearer {EXPECTED_BEARER_TOKEN}"):
        raise HTTPException(status_code=401, detail="Invalid or missing Bearer token")
    return x_token

@app.on_event("startup")
async def load_model_on_startup():
    global tokenizer, model
    if DEVICE == "cpu" and LLM_PRECISION != "cpu_debug": # Allow CPU for debug without full model
        print("WARNING: CUDA not available. Model loading will be slow and likely unusable for production.")
        # Optionally, prevent startup if CUDA is essential
        # raise RuntimeError("CUDA not available, cannot start model service for GPU operations.")

    try:
        print(f"Loading tokenizer: {MODEL_NAME}")
        tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, trust_remote_code=True)
        print(f"Loading model: {MODEL_NAME} to device: {DEVICE} with precision: {LLM_PRECISION}")
        
        # Explicit GPTQConfig as per checklist
        # Note: group_size and desc_act might need to match how the model was quantized.
        # desc_act=True is common, group_size=128 is also common.
        # If the model's own quantization_config.json is sufficient, this explicit config might not be needed
        # or could even conflict if not matched properly. Test with and without if issues arise.
        gptq_quant_config = GPTQConfig(
            bits=4, 
            group_size=128, 
            desc_act=True, #  For some models this might be False
            # tokenizer=tokenizer # Sometimes passed during quantization, not always for loading
        )

        model = AutoModelForCausalLM.from_pretrained(
            MODEL_NAME,
            device_map="auto", # Handles GPU mapping
            torch_dtype=torch.float16, # GPTQ models often loaded in float16
            trust_remote_code=True,
            quantization_config=gptq_quant_config # Explicitly pass it as per checklist
        )
        model.eval() # Set to evaluation mode
        print("Model and tokenizer loaded successfully.")

        # Initial VRAM check (optional, more detailed in /healthz)
        if torch.cuda.is_available():
            allocated_memory = torch.cuda.memory_allocated(DEVICE) / (1024**3)
            print(f"  Initial VRAM Allocated by tensors: {allocated_memory:.2f} GB")

    except Exception as e:
        print(f"Error loading model: {e}")
        # Consider exiting if model load fails critically
        raise RuntimeError(f"Failed to load model: {e}")

# --- Pydantic Models for API --- 
class StyleRequest(BaseModel):
    text: str

class StyleResponse(BaseModel):
    styles: list[str]
    raw_llm_output: str # For debugging

class ExtractRequest(BaseModel):
    html_content: str
    # extraction_schema: dict # Future: pass a schema for the LLM to follow

class ExtractResponse(BaseModel):
    extracted_data: dict
    raw_llm_output: str # For debugging

class DedupeRequest(BaseModel):
    event_text_1: str
    event_text_2: str

class DedupeResponse(BaseModel):
    is_same_event: bool
    confidence: float # Optional
    raw_llm_output: str # For debugging

class HealthResponse(BaseModel):
    status: str
    model_name: str
    device: str
    vram_allocated_tensors_gb: float | None = None
    vram_used_by_pytorch_gb: float | None = None
    vram_total_gb: float | None = None

# --- Helper for LLM Interaction --- 
def parse_qwen_output(raw_response_text: str):
    """Parses the <think>...</think> block if present."""
    # Simplest parsing: raw.split("</think>")[-1]
    # Then strip leading </s> tokens. This depends on the exact tokenizer behavior.
    final_content = raw_response_text
    thinking_content = ""
    if "</think>" in raw_response_text:
        parts = raw_response_text.split("</think>", 1)
        if "<think>" in parts[0]:
            thinking_content = parts[0].split("<think>", 1)[1].strip()
        final_content = parts[1].strip()
        # Remove potential leading/trailing special tokens if tokenizer doesn't handle them well
        if final_content.startswith(tokenizer.eos_token):
            final_content = final_content[len(tokenizer.eos_token):].strip()
    return {"thinking": thinking_content, "final": final_content.strip()}

async def generate_qwen_response_async(prompt_text: str, max_new_tokens: int = 512, enable_thinking: bool = True):
    if not tokenizer or not model:
        raise HTTPException(status_code=503, detail="Model not loaded or still initializing.")

    # Logging the first 50 chars of prompt (as per checklist)
    print(f"[PROMPT_LOG] {prompt_text[:50]}...") 
    start_time = time.time()

    messages = [{"role": "user", "content": prompt_text}]
    chat_ml_text = tokenizer.apply_chat_template(
        messages,
        tokenize=False,
        add_generation_prompt=True,
        # enable_thinking for apply_chat_template might be specific to some Qwen setups/versions.
        # The primary control is usually just the prompt structure for instruct models.
        # The model card for Qwen3 suggests `enable_thinking` is a parameter for `apply_chat_template`.
        # Let's assume it's available, if not, it might be ignored or error.
        # enable_thinking=enable_thinking # This argument might not be standard for all Qwen versions in `apply_chat_template`
    )
    
    model_inputs = tokenizer([chat_ml_text], return_tensors="pt").to(DEVICE)

    generation_kwargs = {
        "max_new_tokens": max_new_tokens,
        "temperature": 0.6 if enable_thinking else 0.7, # Based on Qwen3 best practices
        "top_p": 0.95 if enable_thinking else 0.8,
        "top_k": 20,
        "do_sample": True
    }

    # Wrap blocking I/O (GPU compute) in to_thread
    try:
        generated_ids = await asyncio.to_thread(
            model.generate, 
            **model_inputs, 
            **generation_kwargs
        )
    except Exception as e:
        print(f"Error during model.generate: {e}")
        raise HTTPException(status_code=500, detail=f"LLM generation error: {e}")
    
    output_ids = generated_ids[0][len(model_inputs.input_ids[0]):].tolist()
    raw_response = tokenizer.decode(output_ids, skip_special_tokens=False) # Keep special for think parsing
    
    latency = time.time() - start_time
    print(f"[LATENCY_LOG] {latency:.2f}s for prompt: {prompt_text[:50]}...")

    parsed_output = parse_qwen_output(raw_response)
    return parsed_output['final'] # Return only the final content after thought block

# --- API Endpoints --- 
@app.get("/healthz", response_model=HealthResponse, dependencies=[Depends(verify_token)])
async def health_check():
    vram_allocated_tensors, vram_used_pytorch, vram_total = None, None, None
    if torch.cuda.is_available():
        vram_total = torch.cuda.get_device_properties(DEVICE).total_memory / (1024**3)
        vram_allocated_tensors = torch.cuda.memory_allocated(DEVICE) / (1024**3)
        # For memory reserved by PyTorch (total used by the allocator)
        vram_used_pytorch = torch.cuda.memory_reserved(DEVICE) / (1024**3)
        
    return HealthResponse(
        status="OK" if model and tokenizer else "Model not loaded",
        model_name=MODEL_NAME,
        device=DEVICE,
        vram_allocated_tensors_gb=vram_allocated_tensors,
        vram_used_by_pytorch_gb=vram_used_pytorch, 
        vram_total_gb=vram_total
    )

@app.post("/style", response_model=StyleResponse, dependencies=[Depends(verify_token)])
async def style_endpoint(request: StyleRequest = Body(..., embed=True)):
    allowed_styles_example = ["Salsa", "Bachata", "Kizomba", "Zouk", "Tango", "Swing", "Other", "Unknown"]
    prompt = (
        f"You are an expert dance style classifier. Based on the text below, identify all applicable dance styles. "
        f"Respond ONLY with a valid JSON list of strings. The strings must be from the following allowed styles: {allowed_styles_example}. "
        f"If no style is clear or applicable from the list, return [\"Unknown\"]. Text: \n"
        f"\"\"{request.text}\"\""
    )
    try:
        llm_response_str = await generate_qwen_response_async(prompt, max_new_tokens=150)
        # Robust JSON parsing
        try:
            # The LLM might return a string that looks like a list, but not perfect JSON.
            # Try to clean it up. A more robust way is to ask LLM for JSON object `{"styles": [...]}`
            # For now, simple parsing based on the prompt's request for a JSON list.
            if llm_response_str.strip().startswith("[") and llm_response_str.strip().endswith("]"):
                identified_styles = json.loads(llm_response_str.strip())
                if not isinstance(identified_styles, list):
                    identified_styles = ["parsing_error_not_a_list"]
            else:
                identified_styles = ["parsing_error_not_a_json_list"]
        except json.JSONDecodeError:
            identified_styles = ["json_decode_error"]
        
        return StyleResponse(styles=identified_styles, raw_llm_output=llm_response_str)
    except Exception as e:
        print(f"Error in /style endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/extract", response_model=ExtractResponse, dependencies=[Depends(verify_token)])
async def extract_endpoint(request: ExtractRequest = Body(..., embed=True)):
    json_schema_example = json.dumps({
        "event_name": "string or null",
        "date_start": "YYYY-MM-DD or null",
        "time_start": "HH:MM or null",
        "venue_name": "string or null",
        "address": "string or null",
        "description": "string (concise summary) or null",
        "organizer_name": "string or null",
        "event_url": "URL string or null",
        "ticket_url": "URL string or null",
        "raw_price_info": "string or null"
    }, indent=2)

    prompt = (
        f"You are an expert event information extractor. From the HTML content below, extract the event details. "
        f"Respond ONLY with a valid JSON object that strictly adheres to the following schema. Do not add any commentary before or after the JSON. "
        f"If a field is not found, use a JSON null value, not the string 'null'.\n\n"
        f"JSON Schema to follow:\n{json_schema_example}\n\n"
        f"HTML Content:\n"
        f"<html>\n{request.html_content}\n</html>"
    )
    try:
        llm_response_str = await generate_qwen_response_async(prompt, max_new_tokens=2048) # Allow more for HTML + JSON
        try:
            extracted_data = json.loads(llm_response_str.strip()) # Strip whitespace that might break JSON
        except json.JSONDecodeError:
            # Checklist: "Add a JSON schema validator; retry with higher temp on fail."
            # Retry logic is more complex; for now, return error.
            # print(f"JSONDecodeError for /extract. Raw output: {llm_response_str}")
            extracted_data = {"error": "Failed to parse LLM JSON output", "raw_output_snippet": llm_response_str[:500]}
        return ExtractResponse(extracted_data=extracted_data, raw_llm_output=llm_response_str)
    except Exception as e:
        print(f"Error in /extract endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/dedupe", response_model=DedupeResponse, dependencies=[Depends(verify_token)])
async def dedupe_endpoint(request: DedupeRequest = Body(..., embed=True)):
    prompt = (
        f"Analyze the two event descriptions below. Determine if they refer to the exact same event occurrence. "
        f"Respond ONLY with a valid JSON object in the format: {json.dumps({'same_event': True/False})}. "
        f"Do not add any commentary before or after the JSON.\n\n"
        f"Event 1: \"\"{request.event_text_1}\"\"\n"
        f"Event 2: \"\"{request.event_text_2}\"\""
    )
    try:
        llm_response_str = await generate_qwen_response_async(prompt, max_new_tokens=50)
        try:
            dedupe_result = json.loads(llm_response_str.strip())
            is_same = dedupe_result.get("same_event", False)
            if not isinstance(is_same, bool):
                is_same = False # Default if type is wrong
                # print(f"Dedupe output 'same_event' was not a bool. Raw: {llm_response_str}")
        except json.JSONDecodeError:
            # print(f"JSONDecodeError for /dedupe. Raw output: {llm_response_str}")
            is_same = False # Default on parsing error
        
        # Placeholder confidence, real confidence is hard to get from LLM directly
        confidence = 0.8 if is_same else 0.2 
        return DedupeResponse(is_same_event=is_same, confidence=confidence, raw_llm_output=llm_response_str)
    except Exception as e:
        print(f"Error in /dedupe endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# To run (save as main.py in qwen_inference_service directory):
# Ensure Dockerfile and requirements.txt are in qwen_inference_service directory.
# Build: docker build -t qwen-inference-service ./qwen_inference_service
# Run: docker run -p 8008:8008 -e API_BEARER_TOKEN='your-secret-token' --gpus all qwen-inference-service
# (Or specify MODEL_NAME env var if different from default) 