#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NVIDIA API Model Validation Script

This script helps diagnose which NVIDIA models are available and tests connectivity.
"""

import sys
import io
import openai
import httpx
import json
from datetime import datetime

# Fix Windows encoding issues
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Common NVIDIA model names to test
COMMON_MODELS = [
    "meta/llama-3.3-70b-instruct",
    "meta/llama-3.1-70b-instruct",
    "meta/llama-3.1-405b-instruct",
    "meta/llama-3.1-8b-instruct",
    "nvidia/llama-3.1-nemotron-70b-instruct",
]

def test_model(api_key: str, model_name: str, timeout: int = 30) -> dict:
    """
    Test if a specific model is available and working.
    
    Args:
        api_key: NVIDIA API key
        model_name: Model name to test
        timeout: Timeout in seconds
        
    Returns:
        dict with test results
    """
    print(f"\n{'='*80}")
    print(f"🧪 Testing model: {model_name}")
    print(f"{'='*80}")
    
    try:
        # Configure HTTP client with timeout
        http_client = httpx.Client(
            timeout=httpx.Timeout(
                connect=10.0,
                read=timeout,
                write=10.0,
                pool=10.0
            )
        )
        
        # Configure OpenAI Client
        client = openai.OpenAI(
            base_url="https://integrate.api.nvidia.com/v1",
            api_key=api_key,
            http_client=http_client
        )
        
        # Test with simple message
        start_time = datetime.now()
        print(f"⏰ Starting API call at {start_time.strftime('%H:%M:%S')}")
        
        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Say 'hello' in JSON format with key 'message'."}
            ],
            temperature=0.1,
            max_tokens=50,
            response_format={"type": "json_object"}
        )
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        # Parse response
        content = response.choices[0].message.content
        
        print(f"✅ SUCCESS in {elapsed:.2f} seconds")
        print(f"📝 Response: {content}")
        
        return {
            "model": model_name,
            "status": "success",
            "elapsed_seconds": elapsed,
            "response": content
        }
        
    except httpx.ReadTimeout as e:
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"⏱️ TIMEOUT after {elapsed:.2f} seconds")
        print(f"❌ Error: {str(e)}")
        print(f"💡 This suggests the model may not exist or is extremely slow")
        
        return {
            "model": model_name,
            "status": "timeout",
            "elapsed_seconds": elapsed,
            "error": str(e)
        }
        
    except openai.APIError as e:
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"🔥 API ERROR after {elapsed:.2f} seconds")
        print(f"❌ Error: {str(e)}")
        
        # Try to extract error details
        error_message = str(e)
        if "404" in error_message or "not found" in error_message.lower():
            print(f"💡 This model does NOT exist in NVIDIA's API")
        elif "401" in error_message or "unauthorized" in error_message.lower():
            print(f"💡 API key may be invalid or expired")
        else:
            print(f"💡 Check NVIDIA API documentation for valid model names")
        
        return {
            "model": model_name,
            "status": "api_error",
            "elapsed_seconds": elapsed,
            "error": error_message
        }
        
    except Exception as e:
        elapsed = (datetime.now() - start_time).total_seconds() if 'start_time' in locals() else 0
        print(f"💥 UNEXPECTED ERROR after {elapsed:.2f} seconds")
        print(f"❌ Error type: {type(e).__name__}")
        print(f"❌ Error: {str(e)}")
        
        return {
            "model": model_name,
            "status": "error",
            "elapsed_seconds": elapsed,
            "error": f"{type(e).__name__}: {str(e)}"
        }


def main():
    """Main validation routine."""
    print("\n" + "="*80)
    print("🚀 NVIDIA API Model Validator")
    print("="*80)
    
    # Get API key from user
    print("\n📋 Please provide your NVIDIA API key:")
    print("(Or press Enter to use: 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)")
    api_key = input("API Key: ").strip()
    
    if not api_key:
        api_key = "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"
        print(f"✅ Using default API key")
    
    # Test each model
    results = []
    for model in COMMON_MODELS:
        result = test_model(api_key, model, timeout=45)
        results.append(result)
    
    # Summary
    print("\n" + "="*80)
    print("📊 SUMMARY")
    print("="*80)
    
    working_models = [r for r in results if r['status'] == 'success']
    timeout_models = [r for r in results if r['status'] == 'timeout']
    error_models = [r for r in results if r['status'] in ['api_error', 'error']]
    
    print(f"\n✅ Working models ({len(working_models)}):")
    for r in working_models:
        print(f"   - {r['model']} ({r['elapsed_seconds']:.2f}s)")
    
    print(f"\n⏱️ Timeout models ({len(timeout_models)}):")
    for r in timeout_models:
        print(f"   - {r['model']} (timed out after {r['elapsed_seconds']:.2f}s)")
    
    print(f"\n❌ Failed models ({len(error_models)}):")
    for r in error_models:
        print(f"   - {r['model']}: {r.get('error', 'Unknown error')[:100]}")
    
    print("\n" + "="*80)
    print("💡 RECOMMENDATIONS:")
    print("="*80)
    
    if working_models:
        fastest = min(working_models, key=lambda x: x['elapsed_seconds'])
        print(f"\n✨ Fastest working model: {fastest['model']} ({fastest['elapsed_seconds']:.2f}s)")
        print(f"\n📝 Update your config to use one of the working models:")
        for r in working_models:
            print(f'   "model": "{r["model"]}"')
    
    if timeout_models and not working_models:
        print("\n⚠️ All models timed out. This suggests:")
        print("   1. Network connectivity issues")
        print("   2. NVIDIA API is experiencing high load")
        print("   3. Try again later or increase timeout")
    
    if error_models and "meta/llama-3.3" in str(error_models):
        print("\n⚠️ Llama 3.3 models may not be available yet on NVIDIA API")
        print("   Consider using Llama 3.1 models instead")
    
    # Save results to file
    results_file = "nvidia_model_validation_results.json"
    with open(results_file, 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'results': results
        }, f, indent=2)
    print(f"\n💾 Results saved to: {results_file}")
    print("\n" + "="*80)


if __name__ == "__main__":
    main()
