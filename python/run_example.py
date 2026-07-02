#!/usr/bin/env python3
"""Helper script to run examples."""

import sys
from pathlib import Path

import uvloop

# Add the parent directory to the path so we can import examples
sys.path.insert(0, str(Path(__file__).parent))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run_example.py <example_name>")
        print("Example: python run_example.py auto_chunking_test")
        sys.exit(1)

    example_name = sys.argv[1]

    # Import and run the example
    try:
        if example_name == "auto_chunking":
            from examples.auto_chunking import main

            uvloop.run(main())
        elif example_name == "channel_communication":
            from examples.channel_communication import main

            uvloop.run(main())
        elif example_name == "channel_native_request":
            from examples.channel_native_request import main

            uvloop.run(main())
        elif example_name == "concurrent":
            from examples.concurrent_ops import main

            uvloop.run(main())
        elif example_name == "cross_language_test":
            from examples.cross_language import main

            uvloop.run(main(sys.argv[2:]))
        elif example_name == "generator_types":
            from examples.generator_types import main

            uvloop.run(main())
        elif example_name == "isolated_connections":
            from examples.isolated_connections import main

            uvloop.run(main())
        elif example_name == "isolated_service_server":
            from examples.isolated_service_server import main

            uvloop.run(main())
        elif example_name == "isolated_service":
            from examples.isolated_service import main

            uvloop.run(main())
        elif example_name == "service":
            from examples.service import main

            uvloop.run(main())
        elif example_name == "private_channel":
            from examples.private_channel import main

            uvloop.run(main())
        elif example_name == "private_channel_2":
            from examples.private_channel_2 import main

            uvloop.run(main())
        elif example_name == "property_decorator":
            from examples.property_decorator import main

            uvloop.run(main())
        elif example_name == "plain_object_properties":
            from examples.plain_object_properties import main

            uvloop.run(main())
        elif example_name == "isolated_handler":
            from examples.isolated_handler import main

            uvloop.run(main())
        elif example_name == "unified_streaming":
            from examples.unified_streaming import main

            uvloop.run(main())
        elif example_name == "service_chunking":
            from examples.service_chunking import main

            uvloop.run(main())
        elif example_name == "multi_service":
            from examples.multi_service import main

            uvloop.run(main())
        elif example_name == "native_request_reply":
            from examples.native_request_reply import main

            uvloop.run(main())
        elif example_name == "large_data_transfer":
            from examples.large_data_transfer import main

            uvloop.run(main())
        elif example_name == "all_in_one_performance":
            from examples.all_in_one_performance import main

            uvloop.run(main())
        elif example_name == "pull_vs_push_generators":
            from examples.pull_vs_push_generators import main

            uvloop.run(main())
        elif example_name == "service_pull_vs_push_generators":
            from examples.service_pull_vs_push_generators import main

            uvloop.run(main())
        elif example_name == "callback_subscription":
            from examples.callback_subscription import main

            uvloop.run(main())
        elif example_name == "pull_callback_basic":
            from examples.pull_callback_basic import main

            uvloop.run(main())
        elif example_name == "pull_callback_backpressure":
            from examples.pull_callback_backpressure import main

            uvloop.run(main())
        elif example_name == "pull_callback_cancellation":
            from examples.pull_callback_cancellation import main

            uvloop.run(main())
        elif example_name == "pull_callback_cross":
            from examples.pull_callback_cross import main

            uvloop.run(main(sys.argv[2:]))
        elif example_name == "pull_callback_mechanism":
            from examples.pull_callback_mechanism import main

            uvloop.run(main())
        elif example_name == "reconfigure":
            from examples.reconfigure import main

            uvloop.run(main())
        elif example_name == "perf_hotpath":
            from examples.perf_hotpath import main

            uvloop.run(main())
        else:
            print(f"Unknown example: {example_name}")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Error running example: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
