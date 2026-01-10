# Hyperliquid Rate Limit Handling Guide

## Overview

The Hyperliquid exchange implementation now includes robust retry logic with exponential backoff to handle rate limiting errors. This prevents trade execution failures due to temporary rate limit issues.

## Rate Limit Error Detection

The implementation automatically detects Hyperliquid-specific rate limit errors by checking for:
- "Too many cumulative requests sent" messages
- Volume-based rate limiting errors
- HTTP 429 responses
- Generic rate limit keywords

### Error Message Parsing

When a rate limit error occurs, the system parses the error message to extract:
- **Requests sent**: Number of requests made
- **Requests allowed**: Maximum requests allowed for current volume
- **Volume traded**: Cumulative trading volume in USDC
- **Requests over limit**: How many requests exceeded the limit

**Example Error:**
```
Too many cumulative requests sent (20629 > 16414) for cumulative volume traded $6415.83
```

**Parsed Information:**
- Requests sent: 20,629
- Requests allowed: 16,414
- Volume traded: $6,415.83
- Need ~$4,215 more volume to free up requests

## Configuration Options

Add these options to your `config.json` under the `exchange` section:

```json
{
  "exchange": {
    "name": "hyperliquid",
    "hyperliquid_max_retries": 5,
    "hyperliquid_retry_delay": 2.0,
    "hyperliquid_backoff_factor": 2.5,
    "hyperliquid_max_retry_delay": 30.0,
    "hyperliquid_request_delay": 0.1
  }
}
```

### Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `hyperliquid_max_retries` | `5` | Maximum number of retry attempts for rate-limited requests |
| `hyperliquid_retry_delay` | `2.0` | Initial delay in seconds before first retry |
| `hyperliquid_backoff_factor` | `2.5` | Multiplier for exponential backoff (delay × factor^attempt) |
| `hyperliquid_max_retry_delay` | `30.0` | Maximum delay in seconds between retries |
| `hyperliquid_request_delay` | `0.1` | Small delay added before each request to prevent rate limits |

### Retry Delay Calculation

The delay between retries follows an exponential backoff pattern:

```
Delay = min(initial_delay × (backoff_factor ^ attempt), max_delay)
```

**Example with defaults:**
- Attempt 0: Initial request
- Attempt 1: 2.0s delay
- Attempt 2: 5.0s delay  (2.0 × 2.5¹)
- Attempt 3: 12.5s delay (2.0 × 2.5²)
- Attempt 4: 30.0s delay (capped at max_delay)
- Attempt 5: 30.0s delay (capped at max_delay)

## Recommended Settings

### Conservative (Lower Trading Volume)
If you're trading with lower volume and hitting rate limits frequently:

```json
{
  "hyperliquid_max_retries": 7,
  "hyperliquid_retry_delay": 3.0,
  "hyperliquid_backoff_factor": 2.0,
  "hyperliquid_max_retry_delay": 45.0,
  "hyperliquid_request_delay": 0.2
}
```

### Aggressive (Higher Trading Volume)
If you're trading with higher volume and need faster execution:

```json
{
  "hyperliquid_max_retries": 3,
  "hyperliquid_retry_delay": 1.5,
  "hyperliquid_backoff_factor": 2.0,
  "hyperliquid_max_retry_delay": 20.0,
  "hyperliquid_request_delay": 0.05
}
```

### Balanced (Default - Recommended)
The default settings provide a good balance:

```json
{
  "hyperliquid_max_retries": 5,
  "hyperliquid_retry_delay": 2.0,
  "hyperliquid_backoff_factor": 2.5,
  "hyperliquid_max_retry_delay": 30.0,
  "hyperliquid_request_delay": 0.1
}
```

## Log Output Examples

### Successful Retry After Rate Limit

```
2025-12-31 23:00:01 WARNING - Hyperliquid rate limit error: Rate limit exceeded: 20629 requests sent > 16414 allowed. Volume traded: $6415.83. Need ~$4215.00 more volume in taker orders to free up requests.
2025-12-31 23:00:01 INFO - Waiting 2.0s before retry 1/5...
2025-12-31 23:00:03 INFO - Retry attempt 1/5 for market buy order on BTC/USDC:USDC
2025-12-31 23:00:03 INFO - Order created successfully: abc123 with status: closed
```

### Multiple Retries with Exponential Backoff

```
2025-12-31 23:00:01 WARNING - Hyperliquid rate limit error: Rate limit exceeded...
2025-12-31 23:00:01 INFO - Waiting 2.0s before retry 1/5...
2025-12-31 23:00:03 INFO - Retry attempt 1/5 for market buy order on ETH/USDC:USDC
2025-12-31 23:00:03 WARNING - Hyperliquid rate limit error: Rate limit exceeded...
2025-12-31 23:00:03 INFO - Waiting 5.0s before retry 2/5...
2025-12-31 23:00:08 INFO - Retry attempt 2/5 for market buy order on ETH/USDC:USDC
2025-12-31 23:00:08 INFO - Order created successfully: def456 with status: closed
```

### Max Retries Exceeded

```
2025-12-31 23:00:01 WARNING - Hyperliquid rate limit error: Rate limit exceeded...
2025-12-31 23:00:01 INFO - Waiting 2.0s before retry 1/5...
... (retries continue) ...
2025-12-31 23:01:30 ERROR - Max retries (5) exceeded for order on BTC/USDC:USDC
2025-12-31 23:01:30 ERROR - Hyperliquid rate limit exceeded after 5 retries: Too many cumulative requests sent...
```

## How It Works

### 1. Order Creation Flow

```
┌─────────────────────┐
│ Create Order        │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Add Request Delay   │ (hyperliquid_request_delay)
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Send to Exchange    │
└──────────┬──────────┘
           │
           ▼
      Success?
           │
    ┌──────┴──────┐
   Yes            No
    │              │
    ▼              ▼
  Return    Rate Limit Error?
            ┌──────┴──────┐
           Yes            No
            │              │
            ▼              ▼
    Retries Left?    Raise Error
    ┌──────┴──────┐
   Yes            No
    │              │
    ▼              ▼
Wait with      Raise Error
Backoff        (max retries)
    │
    └─────── Loop back ─────┐
                            │
                            ▼
                    Retry Request
```

### 2. Rate Limit Detection

The system checks for rate limit errors in multiple places:
- `TemporaryError` exceptions
- `ExchangeError` exceptions  
- Error message content analysis

### 3. Retry Mechanism

When a rate limit error is detected:
1. Parses error message to extract details
2. Logs actionable information
3. Calculates exponential backoff delay
4. Waits for the calculated delay
5. Retries the request
6. Repeats until success or max retries reached

## Solving Rate Limit Issues

### Understanding Hyperliquid's Rate Limiting

Hyperliquid uses **volume-based rate limiting**:
- You get a certain number of requests based on your trading volume
- Formula: `allowed_requests ≈ volume_traded_usdc`
- **Placing taker orders** (market orders) increases your allowance
- Each $1 USDC traded as a taker frees up ~1 request

### Solutions

#### 1. **Increase Trading Volume**
Place more taker (market) orders to increase your request allowance:
```python
# The more volume you trade, the more requests you can make
# $1 USDC traded ≈ 1 additional request allowed
```

#### 2. **Adjust Retry Settings**
Increase retry attempts and delays for better tolerance:
```json
{
  "hyperliquid_max_retries": 7,
  "hyperliquid_retry_delay": 3.0,
  "hyperliquid_backoff_factor": 2.5
}
```

#### 3. **Add Request Throttling**
Increase the delay between requests:
```json
{
  "hyperliquid_request_delay": 0.2
}
```

#### 4. **Reduce Order Frequency**
Adjust your strategy to place fewer orders:
- Increase `process_throttle_secs` in config
- Use limit orders instead of frequent market orders
- Combine multiple signals into single orders

#### 5. **Monitor Rate Limit Status**
Watch your logs for rate limit warnings:
```bash
# Check logs for rate limit information
grep "Rate limit exceeded" freqtrade.log
grep "Need ~$" freqtrade.log
```

## Testing the Implementation

### Manual Test

You can trigger a test by temporarily setting very low retry values:

```json
{
  "hyperliquid_max_retries": 2,
  "hyperliquid_retry_delay": 1.0
}
```

Watch the logs during trading to see retry behavior.

### Expected Behavior

1. **Normal operation**: Orders execute without retries
2. **Temporary rate limit**: Orders retry and eventually succeed
3. **Persistent rate limit**: Orders fail after max retries with clear error message

## Troubleshooting

### Issue: Still getting rate limit errors

**Solution:**
1. Increase `hyperliquid_max_retries` to 7-10
2. Increase `hyperliquid_retry_delay` to 3.0-5.0
3. Check if you need to increase trading volume

### Issue: Orders are too slow

**Solution:**
1. Reduce `hyperliquid_retry_delay` to 1.5-2.0
2. Reduce `hyperliquid_request_delay` to 0.05
3. Consider if rate limits are appropriate for your volume

### Issue: Too many retries wasting time

**Solution:**
1. Reduce `hyperliquid_max_retries` to 3
2. Increase actual trading volume to get more request allowance
3. Consider switching to limit orders

## Integration with Existing Configuration

Your existing config file (`config_HL_vf.json`) already has:

```json
{
  "exchange": {
    "ccxt_config": {
      "enableRateLimit": true,
      "rateLimit": 200
    }
  }
}
```

Add the new Hyperliquid-specific settings alongside:

```json
{
  "exchange": {
    "ccxt_config": {
      "enableRateLimit": true,
      "rateLimit": 200
    },
    "hyperliquid_max_retries": 5,
    "hyperliquid_retry_delay": 2.0,
    "hyperliquid_backoff_factor": 2.5,
    "hyperliquid_max_retry_delay": 30.0,
    "hyperliquid_request_delay": 0.1
  }
}
```

## Summary

The Hyperliquid implementation now includes:

✅ **Automatic retry** with exponential backoff  
✅ **Smart error detection** for rate limit messages  
✅ **Detailed logging** with actionable information  
✅ **Request throttling** to prevent rate limits  
✅ **Configurable behavior** for different trading styles  
✅ **Preservation** of all existing functionality  

This makes your trading bot resilient to Hyperliquid's volume-based rate limiting while maintaining fast execution when possible.
