#include "storage/fts/ik/ob_ik_char_util.h"
#include <cassert>
#include <iostream>
#include <string>
using namespace oceanbase::common;
using namespace oceanbase::storage;
int main() {
  for(unsigned int c=0;c<=0x10ffff;++c) {
    if(c>=0xd800 && c<=0xdfff)continue;
    std::string text;
    if(c<0x80)text.push_back(static_cast<char>(c));
    else if(c<0x800){text.push_back(0xc0|(c>>6));text.push_back(0x80|(c&63));}
    else if(c<0x10000){text.push_back(0xe0|(c>>12));text.push_back(0x80|((c>>6)&63));text.push_back(0x80|(c&63));}
    else{text.push_back(0xf0|(c>>18));text.push_back(0x80|((c>>12)&63));text.push_back(0x80|((c>>6)&63));text.push_back(0x80|(c&63));}
    ObFTCharUtil::CharType expected=ObFTCharUtil::CharType::USELESS,actual;
    if(ObUnicodeBlockUtils::is_alpha(c))expected=ObFTCharUtil::CharType::ENGLISH_LETTER;
    else if(ObUnicodeBlockUtils::is_arabic(c))expected=ObFTCharUtil::CharType::ARABIC_LETTER;
    else if(ObUnicodeBlockUtils::is_chinese(c))expected=ObFTCharUtil::CharType::CHINESE;
    else if(ObUnicodeBlockUtils::is_other_cjk(c))expected=ObFTCharUtil::CharType::OTHER_CJK;
    int ret=ObFTCharUtil::classify_first_char(CS_TYPE_UTF8MB4_BIN,text.data(),text.size(),actual);
    assert(ret==0 && actual==expected);
  }
  std::cout<<"ALL_UNICODE_SCALAR_CLASSIFICATION_EQUIVALENCE_PASS\n";
}
