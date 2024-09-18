from langchain_community.chat_models import ChatOllama
from langchain_core.prompts import ChatPromptTemplate


def split_glued_words(glued_text: str) -> str:
    """Converts glued words to separate words, by using the Ollama model.
        :param glued_text: The text with glued words.
        :return: The text with the glued words separated.
    """
    llm = ChatOllama(model="llama3.1:latest")

    prompt = ChatPromptTemplate.from_template("""
    You're an expert at splitting glued words.
    Keep numbers as they are.
    If there is no glued word in the text, return the text as is.
    Here's some examples:
     - ProductDimensions becomes product dimension
     - Manufacturerrecommendedage becomes manufacturer recommended age
         
    Here's the text to rewrite: {glued_text}
    Please rewrite it without any explanation.
    """)
    chain = prompt | llm
    output = chain.invoke({"glued_text": glued_text})
    return output.content


if __name__ == "__main__":
    text = "3x3.8x3inches"
    rewritten = split_glued_words(glued_text=text)
    print(f"{text} -> {rewritten}")
