import beaver_build as bb

# An artifact that "just exists" and isn't generated.
exists = bb.Artifact("exists")

# Generate some arbitrary inputs.
with bb.Group('pre'):
    inputs = bb.transforms._Sleep(["input1.txt", "input2.txt"], None, sleep=0)

# Concatenate the inputs.
output = bb.Shell("output.txt", inputs, "cat $^ > $@")
