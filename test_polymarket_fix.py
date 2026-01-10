#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Polymarket Gamma API Integration Test Script
=============================================

Tests the updated PolymarketAPI class to verify:
- API connection and market fetching
- Actual probabilities are returned (not "N/A")
- Price extraction works correctly
- Sentiment calculation functions properly

Run: python test_polymarket_fix.py
"""

import sys
import os
import json
from datetime import datetime
from pathlib import Path

# Configure output encoding for Windows console
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Add the user_data/strategies directory to path
sys.path.insert(0, str(Path(__file__).parent / "user_data" / "strategies"))

from LlamaGPTStrategy_Phase5 import PolymarketAPI


def print_header(text: str):
    """Print a formatted section header"""
    print("\n" + "=" * 70)
    print(text.center(70))
    print("=" * 70)


def print_test(test_name: str):
    """Print a test section header"""
    print(f"\n{test_name}")
    print("-" * 70)


def print_success(message: str):
    """Print success message"""
    print(f"✓ SUCCESS: {message}")


def print_failure(message: str):
    """Print failure message"""
    print(f"✗ FAILURE: {message}")


def print_market(market: dict, index: int = None):
    """Print a single market in readable format"""
    prefix = f"Market {index}: " if index is not None else "Market: "
    print(f"\n{prefix}")
    print(f"  Question: {market.get('question', 'N/A')}")
    print(f"  Active: {market.get('active', False)}")
    print(f"  Outcomes:")
    
    for outcome in market.get('outcomes', []):
        name = outcome.get('name', 'Unknown')
        price = outcome.get('price', 0)
        # Price is in decimal format (0.0-1.0), convert to percentage
        percentage = price * 100
        print(f"    - {name}: {percentage:.1f}%")


def test_get_crypto_markets(api: PolymarketAPI) -> dict:
    """Test 1: Fetch crypto markets"""
    print_test("Test 1: Fetching crypto markets...")
    
    success, markets = api.get_crypto_markets(limit=10)
    
    result = {
        'success': success,
        'markets_count': len(markets) if success else 0,
        'markets': markets[:3] if success else [],  # Save only first 3
        'has_valid_probabilities': False
    }
    
    if not success:
        print_failure(f"Failed to fetch crypto markets")
        return result
    
    print_success(f"Found {len(markets)} crypto markets")
    
    if markets:
        # Check for valid probabilities
        has_valid_probs = True
        for market in markets:
            for outcome in market.get('outcomes', []):
                price = outcome.get('price')
                if price is None or price == 0:
                    has_valid_probs = False
                    break
        
        result['has_valid_probabilities'] = has_valid_probs
        
        # Display first 3 markets
        for i, market in enumerate(markets[:3], 1):
            print_market(market, i)
        
        if has_valid_probs:
            print_success("All markets have valid probabilities (no N/A values)")
        else:
            print_failure("Some markets have invalid probabilities")
    else:
        print_failure("No markets returned")
    
    return result


def test_search_bitcoin(api: PolymarketAPI) -> dict:
    """Test 2: Search for Bitcoin markets"""
    print_test("Test 2: Searching for Bitcoin markets...")
    
    success, markets = api.search_markets('Bitcoin', limit=5)
    
    result = {
        'success': success,
        'markets_count': len(markets) if success else 0,
        'markets': markets[:2] if success else [],  # Save only first 2
        'has_valid_probabilities': False
    }
    
    if not success:
        print_failure("Failed to search for Bitcoin markets")
        return result
    
    print_success(f"Found {len(markets)} Bitcoin-related markets")
    
    if markets:
        # Check for valid probabilities
        has_valid_probs = True
        for market in markets:
            for outcome in market.get('outcomes', []):
                price = outcome.get('price')
                if price is None or price == 0:
                    has_valid_probs = False
                    break
        
        result['has_valid_probabilities'] = has_valid_probs
        
        # Display first 2 markets
        for i, market in enumerate(markets[:2], 1):
            print_market(market, i)
        
        if has_valid_probs:
            print_success("All Bitcoin markets have valid probabilities")
        else:
            print_failure("Some Bitcoin markets have invalid probabilities")
    else:
        print_failure("No Bitcoin markets found")
    
    return result


def test_search_ethereum(api: PolymarketAPI) -> dict:
    """Test 3: Search for Ethereum markets"""
    print_test("Test 3: Searching for Ethereum markets...")
    
    success, markets = api.search_markets('Ethereum', limit=5)
    
    result = {
        'success': success,
        'markets_count': len(markets) if success else 0,
        'markets': markets[:2] if success else [],  # Save only first 2
        'has_valid_probabilities': False
    }
    
    if not success:
        print_failure("Failed to search for Ethereum markets")
        return result
    
    print_success(f"Found {len(markets)} Ethereum-related markets")
    
    if markets:
        # Check for valid probabilities
        has_valid_probs = True
        for market in markets:
            for outcome in market.get('outcomes', []):
                price = outcome.get('price')
                if price is None or price == 0:
                    has_valid_probs = False
                    break
        
        result['has_valid_probabilities'] = has_valid_probs
        
        # Display first 2 markets
        for i, market in enumerate(markets[:2], 1):
            print_market(market, i)
        
        if has_valid_probs:
            print_success("All Ethereum markets have valid probabilities")
        else:
            print_failure("Some Ethereum markets have invalid probabilities")
    else:
        print_failure("No Ethereum markets found")
    
    return result


def test_extract_market_info(api: PolymarketAPI) -> dict:
    """Test 4: Verify price extraction works"""
    print_test("Test 4: Testing market info extraction...")
    
    # First get a market to test extraction on
    success, markets = api.get_crypto_markets(limit=1)
    
    result = {
        'success': False,
        'extraction_works': False,
        'has_prices': False,
        'sample_market': None
    }
    
    if not success or not markets:
        print_failure("Could not fetch markets for extraction test")
        return result
    
    raw_market = markets[0]
    
    # The get_crypto_markets already uses extract_market_info internally,
    # so if we have a market, extraction is working
    if raw_market and 'outcomes' in raw_market:
        result['success'] = True
        result['extraction_works'] = True
        
        # Check if outcomes have valid prices
        has_prices = all(
            outcome.get('price') is not None and 
            isinstance(outcome.get('price'), (int, float)) and
            0.0 <= outcome.get('price') <= 1.0
            for outcome in raw_market.get('outcomes', [])
        )
        
        result['has_prices'] = has_prices
        result['sample_market'] = raw_market
        
        print_success("Market extraction is working")
        print_success(f"Market has {len(raw_market.get('outcomes', []))} outcomes")
        
        if has_prices:
            print_success("All outcomes have valid prices (0.0-1.0 range)")
            print_market(raw_market)
        else:
            print_failure("Some outcomes have invalid prices")
    else:
        print_failure("Market extraction failed - missing outcomes")
    
    return result


def test_calculate_sentiment(api: PolymarketAPI) -> dict:
    """Test 5: Calculate sentiment from markets"""
    print_test("Test 5: Testing sentiment calculation...")
    
    # Get markets for sentiment analysis
    success, markets = api.get_crypto_markets(limit=10)
    
    result = {
        'success': False,
        'sentiment_data': None
    }
    
    if not success:
        print_failure("Could not fetch markets for sentiment calculation")
        return result
    
    sentiment = api.calculate_sentiment(markets)
    
    if sentiment:
        result['success'] = True
        result['sentiment_data'] = sentiment
        
        print_success("Sentiment calculation completed")
        print(f"\n  Sentiment Breakdown:")
        print(f"    Overall Sentiment: {sentiment.get('sentiment', 'N/A')}")
        print(f"    Bullish Markets: {sentiment.get('bullish_markets', 0)}")
        print(f"    Bearish Markets: {sentiment.get('bearish_markets', 0)}")
        print(f"    Neutral Markets: {sentiment.get('neutral_markets', 0)}")
        print(f"    Avg Bullish Probability: {sentiment.get('avg_bullish_prob', 0):.2%}")
        
        total_markets = sum([
            sentiment.get('bullish_markets', 0),
            sentiment.get('bearish_markets', 0),
            sentiment.get('neutral_markets', 0)
        ])
        
        if total_markets > 0:
            print_success(f"Analyzed {total_markets} markets")
        else:
            print_failure("No markets were analyzed")
    else:
        print_failure("Sentiment calculation returned empty result")
    
    return result


def generate_summary(results: dict) -> dict:
    """Generate test summary"""
    summary = {
        'timestamp': datetime.now().isoformat(),
        'api_connection': results['test1']['success'],
        'market_fetching': results['test1']['success'] and results['test1']['markets_count'] > 0,
        'price_extraction': results['test4']['extraction_works'],
        'probabilities_valid': all([
            results['test1'].get('has_valid_probabilities', False),
            results['test2'].get('has_valid_probabilities', False),
            results['test3'].get('has_valid_probabilities', False)
        ]),
        'sentiment_calculation': results['test5']['success'],
        'overall_status': 'PASS'
    }
    
    # Determine overall status
    if not all([
        summary['api_connection'],
        summary['market_fetching'],
        summary['price_extraction'],
        summary['probabilities_valid'],
        summary['sentiment_calculation']
    ]):
        summary['overall_status'] = 'FAIL'
    
    return summary


def print_summary(summary: dict):
    """Print test summary"""
    print_header("TEST SUMMARY")
    
    status_icon = "✓" if summary['overall_status'] == 'PASS' else "✗"
    
    print(f"\n{status_icon} API Connection: {'Success' if summary['api_connection'] else 'Failed'}")
    print(f"{status_icon} Market Fetching: {'Success' if summary['market_fetching'] else 'Failed'}")
    print(f"{status_icon} Price Extraction: {'Success' if summary['price_extraction'] else 'Failed'}")
    print(f"{status_icon} Probabilities: {'Valid (no N/A values)' if summary['probabilities_valid'] else 'Invalid'}")
    print(f"{status_icon} Sentiment Calculation: {'Success' if summary['sentiment_calculation'] else 'Failed'}")
    
    print("\n" + "=" * 70)
    if summary['overall_status'] == 'PASS':
        print("✓ ALL TESTS PASSED - The integration is working correctly!".center(70))
    else:
        print("✗ SOME TESTS FAILED - Please review the errors above".center(70))
    print("=" * 70)


def save_results(results: dict, summary: dict):
    """Save test results to JSON file"""
    output = {
        'test_run': {
            'timestamp': summary['timestamp'],
            'overall_status': summary['overall_status']
        },
        'summary': summary,
        'detailed_results': results
    }
    
    output_file = Path(__file__).parent / 'polymarket_test_results.json'
    
    try:
        with open(output_file, 'w') as f:
            json.dump(output, f, indent=2)
        print(f"\n✓ Results saved to: {output_file}")
    except Exception as e:
        print(f"\n✗ Failed to save results: {e}")


def main():
    """Main test execution"""
    print_header("Testing Polymarket Gamma API Integration")
    print(f"\nTest started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Testing PolymarketAPI class from LlamaGPTStrategy_Phase5.py")
    
    # Initialize API (no API key needed for public endpoints)
    api = PolymarketAPI()
    
    # Run all tests
    results = {
        'test1': test_get_crypto_markets(api),
        'test2': test_search_bitcoin(api),
        'test3': test_search_ethereum(api),
        'test4': test_extract_market_info(api),
        'test5': test_calculate_sentiment(api)
    }
    
    # Generate and print summary
    summary = generate_summary(results)
    print_summary(summary)
    
    # Save results to file
    save_results(results, summary)
    
    # Return exit code
    return 0 if summary['overall_status'] == 'PASS' else 1


if __name__ == '__main__':
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n✗ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
