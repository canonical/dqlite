#include "id.h"

#include <string.h>

/* The PRNG used for generating request IDs is xoshiro256**, developed by
 * David Blackman and Sebastiano Vigna and released into the public domain.
 * See <https://xoshiro.di.unimi.it/xoshiro256starstar.c>. */

static uint64_t rotl(uint64_t x, int k)
{
	return (x << k) | (x >> (64 - k));
}

uint64_t idNext(struct id_state *state)
{
	uint64_t result = rotl(state->data[1] * 5, 7) * 9;
	uint64_t t = state->data[1] << 17;

	state->data[2] ^= state->data[0];
	state->data[3] ^= state->data[1];
	state->data[1] ^= state->data[2];
	state->data[0] ^= state->data[3];

	state->data[2] ^= t;

	state->data[3] = rotl(state->data[3], 45);

	return result;
}

void idJump(struct id_state *state)
{
	static const uint64_t JUMP[] = {0x180ec6d33cfd0aba, 0xd5a61266f0c9392c,
					0xa9582618e03fc9aa, 0x39abdc4529b1661c};

	uint64_t s0 = 0;
	uint64_t s1 = 0;
	uint64_t s2 = 0;
	uint64_t s3 = 0;
	for (size_t i = 0; i < sizeof(JUMP) / sizeof(*JUMP); i++) {
		for (size_t b = 0; b < 64; b++) {
			if (JUMP[i] & UINT64_C(1) << b) {
				s0 ^= state->data[0];
				s1 ^= state->data[1];
				s2 ^= state->data[2];
				s3 ^= state->data[3];
			}
			idNext(state);
		}
	}

	state->data[0] = s0;
	state->data[1] = s1;
	state->data[2] = s2;
	state->data[3] = s3;
}

uint64_t idExtract(const uint8_t buf[16])
{
	uint64_t id;
	memcpy(&id, buf, sizeof(id));
	return id;
}

void idSet(uint8_t buf[16], uint64_t id)
{
	memset(buf, 0, 16);
	memcpy(buf, &id, sizeof(id));
	buf[15] = (uint8_t)-1;
}
