import beaver_build as bb


# Generate some arbitrary inputs.
inputs = bb.Sleep(["input1.txt", "input2.txt"], None, time=0)

# Concatenate the inputs.
output = bb.Shell("output.txt", inputs, "cat $^ > $@")
