#!/usr/bin/env python3
"""Test script for segment grouping logic"""

from src.rabbitmq import RabbitMQConnection

# Test data
segments = [
    {"start_ms": 0, "end_ms": 3500, "text": "Hello"},
    {"start_ms": 3500, "end_ms": 7500, "text": "world"},
    {"start_ms": 7500, "end_ms": 9500, "text": "good"},
    {"start_ms": 9500, "end_ms": 10000, "text": "day"},
]

rmq = RabbitMQConnection()

print("Test 1: min_mark_duration_ms = 6000")
groups = rmq._group_segments_by_duration(segments, 6000)
for i, g in enumerate(groups):
    print(f"  Group {i}: start={g['start_ms']}ms, duration={g['duration_ms']}ms, text='{g['text']}'")

print("\nTest 2: min_mark_duration_ms = 5000")
groups = rmq._group_segments_by_duration(segments, 5000)
for i, g in enumerate(groups):
    print(f"  Group {i}: start={g['start_ms']}ms, duration={g['duration_ms']}ms, text='{g['text']}'")

print("\nTest 3: min_mark_duration_ms = 1000")
groups = rmq._group_segments_by_duration(segments, 1000)
for i, g in enumerate(groups):
    print(f"  Group {i}: start={g['start_ms']}ms, duration={g['duration_ms']}ms, text='{g['text']}'")

print("\nTest 4: Single segment")
single = [{"start_ms": 0, "end_ms": 5000, "text": "Single segment"}]
groups = rmq._group_segments_by_duration(single, 6000)
for i, g in enumerate(groups):
    print(f"  Group {i}: start={g['start_ms']}ms, duration={g['duration_ms']}ms, text='{g['text']}'")

print("\nAll tests completed!")
