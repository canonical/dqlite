#ifdef DEBUG_VERBOSE

void ts (void);

#define DEBUG_MSG(MSG) { 						\
   ts();								\
   printf("%s:%d (%s) -- %s\n", __FILE__, __LINE__, __func__, MSG);	\
   fflush(stdout);							\
}

#define DEBUG_TX(TX, MSG) { 							\
   size_t id = 0; 								\
   if (TX != NULL) {id = ((struct tx*)(TX))->id;}				\
   ts();									\
   printf("%s:%d (%s) TX:%09ld -- %s\n", __FILE__, __LINE__, __func__, id, MSG);	\
   fflush(stdout);								\
}
#else
#define DEBUG_MSG(MSG)
#define DEBUG_TX(TX, MSG)
#endif // DEBUG_VERBOSE

