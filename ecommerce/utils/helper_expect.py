# ---------------------------------------------------------------------------
# Decorator Factory: Apply Expectations Cleanly
# ---------------------------------------------------------------------------

from pyspark import pipelines as sdp
def apply_expectations(expectations: dict):
    """
    Converts expectation config into chained SDP decorators.
    """
    def decorator(fn):
        for name, condition in expectations.get("expect", {}).items():
            fn = sdp.expect(name, condition)(fn)

        for name, condition in expectations.get("expect_or_drop", {}).items():
            fn = sdp.expect_or_drop(name, condition)(fn)

        for name, condition in expectations.get("expect_or_fail", {}).items():
            fn = sdp.expect_or_fail(name, condition)(fn)

        return fn

    return decorator