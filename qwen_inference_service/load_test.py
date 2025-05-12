import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, GPTQConfig
import os

# --- Configuration --- 
# You can change this to "JunHowie/Qwen3-32B-GPTQ-Int4" or other models if needed
DEFAULT_MODEL_NAME = "glide-the/Qwen3-32B-GPTQ-4bits"
MODEL_NAME = os.getenv("MODEL_NAME_TEST", DEFAULT_MODEL_NAME)

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

def check_model_properties_and_vram():
    print(f"Starting model load test for: {MODEL_NAME} on device: {DEVICE}")
    if DEVICE == "cpu":
        print("WARNING: CUDA not available. GPU VRAM usage cannot be checked. Test will proceed on CPU.")

    tokenizer = None
    model = None
    actual_hidden_size_for_32b = 6656 # Standard for Qwen 32B models

    print(f"\nAttempting to load tokenizer: {MODEL_NAME}")
    try:
        tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, trust_remote_code=True)
        print("Tokenizer loaded successfully.")
    except Exception as e:
        print(f"Error loading tokenizer: {e}")
        return

    print(f"\nAttempting to load model: {MODEL_NAME}")
    try:
        # Configuration for loading GPTQ model
        # This explicit config is good for ensuring consistency as per your checklist
        gptq_load_config = GPTQConfig(
            bits=4,
            group_size=128, # Common, but verify if model card specifies differently
            desc_act=True,  # Common, but verify if model card specifies differently
            # dataset=None, # Not needed for loading pre-quantized
        )

        model = AutoModelForCausalLM.from_pretrained(
            MODEL_NAME,
            device_map="auto", 
            torch_dtype=torch.float16,
            trust_remote_code=True,
            quantization_config=gptq_load_config 
        )
        print("Model loaded successfully.")
        model.eval()

        # 1. Verify Model Configuration
        print("\n--- Model Configuration ---")
        if hasattr(model.config, 'hidden_size'):
            print(f"  Model Config - hidden_size: {model.config.hidden_size}")
            if model.config.hidden_size == actual_hidden_size_for_32b:
                print(f"  --> Confirmed: hidden_size matches expected {actual_hidden_size_for_32b} for a Qwen 32B model.")
            else:
                print(f"  --> WARNING: hidden_size is {model.config.hidden_size}, expected {actual_hidden_size_for_32b} for Qwen 32B. Please double-check model variant.")
        else:
            print("  Could not read model.config.hidden_size.")

        for attr in ['num_hidden_layers', 'num_attention_heads', 'vocab_size']:
            if hasattr(model.config, attr):
                print(f"  Model Config - {attr}: {getattr(model.config, attr)}")
            else:
                print(f"  Could not read model.config.{attr}.")
        
        # 2. Check VRAM Usage
        print("\n--- VRAM Usage ---")
        if torch.cuda.is_available():
            torch.cuda.synchronize() 
            allocated_memory_tensors = torch.cuda.memory_allocated(DEVICE) / (1024**3)  
            reserved_memory_pytorch = torch.cuda.memory_reserved(DEVICE) / (1024**3)   
            total_gpu_memory = torch.cuda.get_device_properties(DEVICE).total_memory / (1024**3)
            
            print(f"  VRAM Allocated by tensors: {allocated_memory_tensors:.2f} GB")
            print(f"  VRAM Used by PyTorch (Reserved): {reserved_memory_pytorch:.2f} GB")
            print(f"  Total GPU Memory: {total_gpu_memory:.2f} GB")

            target_vram = 21
            if reserved_memory_pytorch <= target_vram:
                print(f"  --> VRAM usage ({reserved_memory_pytorch:.2f} GB) is within the target of <= {target_vram} GB. Good!")
            else:
                print(f"  --> WARNING: VRAM usage ({reserved_memory_pytorch:.2f} GB) is > {target_vram} GB. May not leave enough headroom for FastAPI + CUDA kernels.")
        else:
            print("  CUDA not available, cannot check VRAM usage directly via torch.")

        # 3. Simple Test Inference
        print("\n--- Test Inference ---")
        prompt_text = "Give me a short introduction to large language models."
        print(f"  Test prompt: \"{prompt_text}\"")
        
        messages = [{"role": "user", "content": prompt_text}]
        chat_ml_text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
        inputs = tokenizer([chat_ml_text], return_tensors="pt").to(DEVICE)
        
        # Use sampling parameters consistent with FastAPI app for thinking mode
        generation_kwargs = {
            "max_new_tokens": 50,
            "temperature": 0.6,
            "top_p": 0.95,
            "top_k": 20,
            "do_sample": True
        }
        outputs = model.generate(**inputs, **generation_kwargs)
        
        # Decode only the newly generated tokens
        output_ids = outputs[0][len(inputs.input_ids[0]):].tolist()
        response_text = tokenizer.decode(output_ids, skip_special_tokens=True).strip()
        print(f"  LLM Response (decoded, new tokens only): \"{response_text}\"")
        print("  Simple generation test complete.")

    except Exception as e:
        print(f"\nError during model loading or testing: {e}")
        if "out of memory" in str(e).lower():
            print("  CUDA Out of Memory error occurred. The model may be too large for the available VRAM with current settings or quantization_config is incorrect.")
        elif "bitsandbytes" in str(e).lower():
             print("  Error might be related to bitsandbytes. Ensure it's not being inadvertently triggered if this is a GPTQ model.")
        elif "autogptq" in str(e).lower() or "GPTQConfig" in str(e):
            print("  Error seems related to AutoGPTQ/GPTQConfig. Ensure `auto-gptq` and `optimum` are installed and `GPTQConfig` parameters (bits, group_size, desc_act) match the model's quantization.")
            print("  Some models might require specific `model_basename` in GPTQConfig if not automatically inferred, or might not need explicit GPTQConfig if `device_map='auto'` handles it.")

    finally:
        del model
        del tokenizer
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        print("\nModel and tokenizer unloaded, CUDA cache cleared (if CUDA was used).")
        print("Test script finished.")

if __name__ == "__main__":
    check_model_properties_and_vram() 