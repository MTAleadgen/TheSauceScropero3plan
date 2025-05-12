#!/usr/bin/env python3
import os
import json
import argparse
import logging
from typing import List, Dict, Any, Optional
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
from pathlib import Path
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_MODEL = "Qwen/Qwen3-7B"
MAX_NEW_TOKENS = 4096
TEMPERATURE = 0.7

# Sample prompt template for extracting dance class events from SERP results
PROMPT_TEMPLATE = """
You are a specialized AI designed to extract dance class events from search engine results.
Your task is to analyze the provided search results and identify structured information about dance classes.

For each search result, extract the following information if available:
1. Event name/title
2. Dance style (salsa, bachata, kizomba, etc.)
3. Date and time
4. Location/venue
5. Organizer/studio name
6. Price/cost (if mentioned)
7. Experience level (beginner, intermediate, advanced)
8. URL or booking link
9. Brief description

Format your response as a JSON array of event objects with the following structure:
[
  {
    "event_name": "string",
    "dance_style": "string",
    "date_time": "string",
    "location": "string",
    "organizer": "string",
    "price": "string",
    "experience_level": "string",
    "url": "string",
    "description": "string"
  }
]

If a field is not available, include it as null.
Only extract actual dance classes or events - ignore generic websites, articles, or non-event content.

Search Results:
{serp_results}

JSON Output (events only):
"""

def load_sample_data(file_path: str) -> Dict[str, Any]:
    """Load sample SERP data from a JSON file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading sample data: {e}")
        return {}

def save_output(data: Any, output_path: str) -> None:
    """Save the parsed output to a file"""
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            if isinstance(data, (dict, list)):
                json.dump(data, f, indent=2)
            else:
                f.write(str(data))
        logger.info(f"Output saved to {output_path}")
    except Exception as e:
        logger.error(f"Error saving output: {e}")

def format_serp_for_prompt(serp_data: Dict[str, Any]) -> str:
    """Format the SERP data for inclusion in the prompt"""
    formatted_results = []
    
    # Extract organic results
    organic_results = serp_data.get("organic_results", [])
    for i, result in enumerate(organic_results):
        formatted_result = f"Result {i+1}:\n"
        formatted_result += f"Title: {result.get('title', 'N/A')}\n"
        formatted_result += f"URL: {result.get('link', 'N/A')}\n"
        formatted_result += f"Snippet: {result.get('snippet', 'N/A')}\n"
        
        # Add rich snippet data if available
        if "rich_snippet" in result:
            rich = result["rich_snippet"]
            if "top" in rich:
                formatted_result += f"Rich Data: {rich['top']}\n"
        
        formatted_results.append(formatted_result)
    
    return "\n\n".join(formatted_results)

def load_model_and_tokenizer(model_name_or_path: str):
    """Load the LLM model and tokenizer"""
    logger.info(f"Loading model and tokenizer from: {model_name_or_path}")
    start_time = time.time()
    
    try:
        # Check if CUDA is available
        device = "cuda" if torch.cuda.is_available() else "cpu"
        logger.info(f"Using device: {device}")
        
        if device == "cuda":
            logger.info(f"CUDA device: {torch.cuda.get_device_name(0)}")
            logger.info(f"CUDA memory allocated: {torch.cuda.memory_allocated(0) / 1e9:.2f} GB")
            logger.info(f"CUDA memory reserved: {torch.cuda.memory_reserved(0) / 1e9:.2f} GB")
        
        # Load model with appropriate configuration for inference
        model = AutoModelForCausalLM.from_pretrained(
            model_name_or_path,
            device_map="auto",
            torch_dtype=torch.bfloat16,
            trust_remote_code=True
        )
        
        # Load tokenizer
        tokenizer = AutoTokenizer.from_pretrained(
            model_name_or_path,
            trust_remote_code=True
        )
        
        elapsed = time.time() - start_time
        logger.info(f"Model loaded in {elapsed:.2f} seconds")
        
        return model, tokenizer
    
    except Exception as e:
        logger.error(f"Error loading model: {e}")
        raise

def generate_response(model, tokenizer, prompt: str, max_new_tokens: int = MAX_NEW_TOKENS, temperature: float = TEMPERATURE) -> str:
    """Generate a response from the model"""
    logger.info("Generating response...")
    start_time = time.time()
    
    try:
        inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
        
        # Log token count
        input_tokens = inputs.input_ids.shape[1]
        logger.info(f"Input token count: {input_tokens}")
        
        # Generate with appropriate parameters
        outputs = model.generate(
            **inputs,
            max_new_tokens=max_new_tokens,
            temperature=temperature,
            do_sample=temperature > 0,
            pad_token_id=tokenizer.eos_token_id
        )
        
        # Decode the output, skipping the input tokens
        response = tokenizer.decode(outputs[0][input_tokens:], skip_special_tokens=True)
        
        elapsed = time.time() - start_time
        logger.info(f"Response generated in {elapsed:.2f} seconds")
        
        return response.strip()
    
    except Exception as e:
        logger.error(f"Error generating response: {e}")
        return ""

def parse_llm_response(response: str) -> List[Dict[str, Any]]:
    """Parse the LLM response into structured data"""
    try:
        # Try to extract JSON from the response
        # Look for JSON array pattern: [...]
        start_idx = response.find('[')
        end_idx = response.rfind(']') + 1
        
        if start_idx >= 0 and end_idx > start_idx:
            json_str = response[start_idx:end_idx]
            events = json.loads(json_str)
            return events
        else:
            logger.warning("Could not find JSON array in response.")
            return []
    
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON from response: {e}")
        logger.error(f"Response was: {response}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error parsing response: {e}")
        return []

def main():
    parser = argparse.ArgumentParser(description="Parse SERP data with Qwen-3 LLM")
    parser.add_argument("--input", required=True, help="Path to sample SERP JSON data")
    parser.add_argument("--output", default="parsed_events.json", help="Path to output file")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Model path or name")
    parser.add_argument("--max-tokens", type=int, default=MAX_NEW_TOKENS, help="Maximum new tokens to generate")
    parser.add_argument("--temperature", type=float, default=TEMPERATURE, help="Sampling temperature")
    parser.add_argument("--skip-gpu-check", action="store_true", help="Skip GPU availability check")
    args = parser.parse_args()
    
    # Check for GPU if not skipped
    if not args.skip_gpu_check and not torch.cuda.is_available():
        logger.warning("CUDA is not available. This script may be very slow on CPU.")
        confirm = input("Continue without GPU? [y/N]: ")
        if confirm.lower() != 'y':
            logger.info("Exiting due to no GPU.")
            return
    
    # Load sample data
    sample_data = load_sample_data(args.input)
    if not sample_data:
        logger.error("No sample data loaded. Exiting.")
        return
    
    # Format SERP data for prompt
    formatted_serp = format_serp_for_prompt(sample_data)
    
    # Create the full prompt
    prompt = PROMPT_TEMPLATE.format(serp_results=formatted_serp)
    
    # Log the prompt (for debugging)
    logger.info(f"Prompt length: {len(prompt)} characters")
    
    try:
        # Load model and tokenizer
        model, tokenizer = load_model_and_tokenizer(args.model)
        
        # Generate response
        response = generate_response(
            model, 
            tokenizer, 
            prompt, 
            max_new_tokens=args.max_tokens,
            temperature=args.temperature
        )
        
        # Parse structured data from response
        events = parse_llm_response(response)
        
        # Save raw response for debugging
        raw_output_path = f"{os.path.splitext(args.output)[0]}_raw.txt"
        save_output(response, raw_output_path)
        
        # Save structured data
        save_output(events, args.output)
        
        # Print summary
        logger.info(f"Extracted {len(events)} events from SERP data")
        logger.info(f"Raw response saved to: {raw_output_path}")
        logger.info(f"Structured data saved to: {args.output}")
        
    except Exception as e:
        logger.error(f"Error running LLM inference: {e}")
    
    logger.info("LLM parsing completed.")

if __name__ == "__main__":
    main() 