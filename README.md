# RaftKV - A Simple Distributed Key-Value Store(For Learning)

**A learning project** implementing a distributed key-value database with:
- **Storage Engine**: Implemented in C (high performance)
- **Consensus Algorithm**: Raft implemented in Go (for distributed coordination)

## Features
- Basic CRUD operations
- Raft consensus for data replication
- Simple network interface
- Educational design (easy to understand and modify)

## Getting Started

### Prerequisites
- Go 1.16+
- GCC or Clang
- Linux/MacOS (Windows may require adjustments)

### Installation
```bash
git clone https://github.com/GDUT-CJL/Raft.git
cd Raft/src/
```

### Build & Run

1. First create a `config.json` (see Configuration section)
2. Build the server:

```bash
go build main.go
```

3. Run:

```bash
./main
```

## Testing the Sysyem

### Using Telnet

```bash
telnet 127.0.0.1 8000
```

Then send commands like:

```bash
SET key value
GET key
DEL key
```

### Using NetWork Tools

You can also use any TCP client like:

- Netcat (`nc`)
- Custom client applications
- Network debugging assistants

### Documentation

Detailed documentation is available on the author's blog:
[C_J_L12580's Blog]([不爱编程的小陈-CSDN博客](https://blog.csdn.net/C_J_L12580?spm=1000.2115.3001.5343)) *(Coming Soon)*

### Contributing

This is a learning project. Contributions are welcome through:

- Issue reports
- Pull requests
- Documentation improvements

## License

[MIT License](https://license/) 