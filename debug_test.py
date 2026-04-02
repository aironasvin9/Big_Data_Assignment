# debug_test.py

# Sample code to test anomaly detection functions without multiprocessing

# Assuming you have a function `detect_anomalies(data)` to test

def test_anomaly_detection():
    # Sample data for testing
    sample_data = [0.1, 0.2, 0.3, 0.5, 5.0, 3.0, 0.2, 0.1]

    # Call the anomaly detection function
    anomalies = detect_anomalies(sample_data)
    
    # Print the detected anomalies
    print("Detected anomalies:", anomalies)

if __name__ == '__main__':
    test_anomaly_detection()