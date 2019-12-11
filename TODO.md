
# Topology Management


# Performance

## Network optimization

### Tuple batching

Pack multiple tuples with the same destination into a single `NetworkTask` object.

### Destination set optimization

Use -1 for broadcast rather than listing all destination ids

### Linger/size trigger

Do not write data to a socket until a certain timeout or size trigger has occurred.

## Remapping

Provide a facility for changing the mapping of a topology based on environment changes.

# Fault tolerance

## Checkpointing





