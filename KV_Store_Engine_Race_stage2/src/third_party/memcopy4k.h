inline void
mov256(uint8_t *dst, const uint8_t *src) {
    asm volatile ("movdqu (%[src]), %%xmm0\n\t"
                  "movdqu 16(%[src]), %%xmm1\n\t"
                  "movdqu 32(%[src]), %%xmm2\n\t"
                  "movdqu 48(%[src]), %%xmm3\n\t"
                  "movdqu 64(%[src]), %%xmm4\n\t"
                  "movdqu 80(%[src]), %%xmm5\n\t"
                  "movdqu 96(%[src]), %%xmm6\n\t"
                  "movdqu 112(%[src]), %%xmm7\n\t"
                  "movdqu 128(%[src]), %%xmm8\n\t"
                  "movdqu 144(%[src]), %%xmm9\n\t"
                  "movdqu 160(%[src]), %%xmm10\n\t"
                  "movdqu 176(%[src]), %%xmm11\n\t"
                  "movdqu 192(%[src]), %%xmm12\n\t"
                  "movdqu 208(%[src]), %%xmm13\n\t"
                  "movdqu 224(%[src]), %%xmm14\n\t"
                  "movdqu 240(%[src]), %%xmm15\n\t"
                  "movdqu %%xmm0, (%[dst])\n\t"
                  "movdqu %%xmm1, 16(%[dst])\n\t"
                  "movdqu %%xmm2, 32(%[dst])\n\t"
                  "movdqu %%xmm3, 48(%[dst])\n\t"
                  "movdqu %%xmm4, 64(%[dst])\n\t"
                  "movdqu %%xmm5, 80(%[dst])\n\t"
                  "movdqu %%xmm6, 96(%[dst])\n\t"
                  "movdqu %%xmm7, 112(%[dst])\n\t"
                  "movdqu %%xmm8, 128(%[dst])\n\t"
                  "movdqu %%xmm9, 144(%[dst])\n\t"
                  "movdqu %%xmm10, 160(%[dst])\n\t"
                  "movdqu %%xmm11, 176(%[dst])\n\t"
                  "movdqu %%xmm12, 192(%[dst])\n\t"
                  "movdqu %%xmm13, 208(%[dst])\n\t"
                  "movdqu %%xmm14, 224(%[dst])\n\t"
                  "movdqu %%xmm15, 240(%[dst])"
    :
    :[src] "r"(src),
    [dst] "r"(dst)
    : "xmm0", "xmm1", "xmm2", "xmm3",
            "xmm4", "xmm5", "xmm6", "xmm7",
            "xmm8", "xmm9", "xmm10", "xmm11",
            "xmm12", "xmm13", "xmm14", "xmm15", "memory");
}

#define mov512(dst, src) mov256(dst, src); \
        mov256(dst + 256, src + 256);

#define mov1024(dst, src) mov512(dst, src); \
        mov512(dst + 512, src + 512);

#define mov2048(dst, src) mov1024(dst, src); \
        mov1024(dst + 1024, src + 1024);

inline void memcpy_4k(void *dst, const void *src) {
    for (int i = 0; i < 16; ++i) {
        mov256((uint8_t *) dst + (i << 8), (uint8_t *) src + (i << 8));
    }
}