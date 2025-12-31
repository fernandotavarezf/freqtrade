#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Quick NVIDIA model availability test"""

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

# Use API key from config
API_KEY = "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"

# Models to test
MODELS_TO_TEST = [
    "meta/llama-3.3-70b-instruct",      # Your current model (likely doesn't exist)
    "meta/llama-3.1-70b-instruct",      # Known working model
    "meta/llama-3.1-405b-instruct",     # Larger known working model
]

def test_model(model_name, timeout=45):
    """Test a single model"""
    print(f"\n{'='*80}")
    print(f"Testing: {model_name}")
    print(f"{'='*80}")
    
    try:
        http_client = httpx.Client(
            timeout=httpx.Timeout(connect=10.0, read=timeout, write=10.0, pool=10.0)
        )
        
        client = openai.OpenAI(
            base_url="https://integrate.api.nvidia.com/v1",
            api_key=API_KEY,
            http_client=http_client
        )
        
        start = datetime.now()
        print(f"Starting call at {start.strftime('%H:%M:%S')}...")
        
        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": "You are helpful."},
                {"role": "user", "content": 'Say {"status":"ok"}'}
            ],
            temperature=0.1,
            max_tokens=20,
            response_format={"type": "json_object"}
        )
        
        elapsed = (datetime.now() - start).total_seconds()
        content = response.choices[0].message.content
        
        print(f"✅ SUCCESS in {elapsed:.2f}s")
        print(f"Response: {content}")
        return {"model": model_name, "status": "success", "time": elapsed}
        
    except httpx.ReadTimeout as e:
        elapsed = (datetime.now() - start).total_seconds()
        print(f"❌ TIMEOUT after {elapsed:.2f}s")
        print(f"Details: {str(e)[:200]}")
        print(f"💡 Model likely does NOT exist or is unavailable")
        return {"model": model_name, "status": "timeout", "time": elapsed}
        
    except openai.APIError as e:
        elapsed = (datetime.now() - start).total_seconds()
        print(f"❌ API ERROR after {elapsed:.2f}s")
        error_str = str(e)
        print(f"Details: {error_str[:200]}")
        
        if "404" in error_str or "not found" in error_str.lower():
            print(f"💡 Model does NOT exist in NVIDIA catalog")
        elif "401" in error_str:
            print(f"💡 Authentication issue")
        
        return {"model": model_name, "status": "error", "time": elapsed, "error": error_str[:200]}
        
    except Exception as e:
        elapsed = (datetime.now() - start).total_seconds() if 'start' in locals() else 0
        print(f"❌ ERROR: {type(e).__name__}")
        print(f"Details: {str(e)[:200]}")
        return {"model": model_name, "status": "error", "time": elapsed, "error": str(e)[:200]}


def main():
    print("\n" + "="*80)
    print("NVIDIA API Model Tester")
    print("="*80)
    print(f"Testing {len(MODELS_TO_TEST)} models with 45s timeout each...")
    
    results = []
    for model in MODELS_TO_TEST:
        result = test_model(model)
        results.append(result)
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    
    working = [r for r in results if r['status'] == 'success']
    failed = [r for r in results if r['status'] != 'success']
    
    if working:
        print(f"\n✅ WORKING MODELS ({len(working)}):")
        for r in working:
            print(f"   • {r['model']} ({r['time']:.1f}s)")
    
    if failed:
        print(f"\n❌ FAILED/UNAVAILABLE ({len(failed)}):")
        for r in failed:
            print(f"   • {r['model']} - {r['status']}")
    
    print("\n" + "="*80)
    print("RECOMMENDATION:")
    print("="*80)
    
    if "meta/llama-3.3-70b-instruct" in [r['model'] for r in failed]:
        print("\n⚠️  Llama 3.3 is NOT available on NVIDIA API yet!")
        print("\n📝 Update your config files to use:")
        if working:
            fastest = min(working, key=lambda x: x['time'])
            print(f'   "model": "{fastest["model"]}"')
        else:
            print('   "model": "meta/llama-3.1-70b-instruct"')
    
    # Save results
    with open("model_test_results.json", 'w') as f:
        json.dump({"timestamp": datetime.now().isoformat(), "results": results}, f, indent=2)
    print("\n💾 Results saved to: model_test_results.json")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
