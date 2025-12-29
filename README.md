# MEMEZ In-Memory Cache in ZIG

## Description

This is just a hobby project to learn me some zig. It is not meant to be used in any serious way nor should be used in production.
It is still very much so work in project and does not even contain any way to communicate with it.

I plan to implement TCP multiplexing and storage persistence, along with some of the functions from Redis.

Right now it uses `Fixed Buffer Allocator`, meaning it'll allocate whatever memory you give it. If you state you need 2G of memory, it will eat that up like
it has been fasting for over a decade. My plan is to move to either a `c_allocator` or just the `General Purpose Allocator`. The `c_allocator` shortcomings are
the fact that it will not detect memory leaks, as I understand.

## Building from source

Just run `zig build` bro. But there's not much there so kind of pointless, ain't it?

